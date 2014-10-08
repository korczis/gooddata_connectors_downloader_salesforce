module GoodData
  module Connectors
    module DownloaderSalesforce

      class SalesForceDownloader < Base::BaseDownloader
        DEFAULT_VERSION = '29.0'
        DEFAULT_MIN_DATE = DateTime.parse("1999-01-01T00:00:00.000Z")
        DEFAULT_VALIDATION_DIRECTORY = File.join(File.dirname(__FILE__),"../validations")


        attr_accessor :client,:bulk_client

        def initialize(metadata,options = {})
          @type = "salesforce_downloader"
          $now = GoodData::Connectors::Metadata::Runtime.now
          super(metadata,options)
        end

        def define_mandatory_configuration
          {
              "salesforce_downloader" => ["username","password","token","client_id","client_secret"]
          }.merge!(super)
        end

        def define_default_configuration
          {
              @type => {}
          }
        end

        def define_default_entities
          [
              "OpportunityFieldHistory" =>
                  {
                    "custom" => {
                        "id" => "OpportunityId",
                        "timestamp" => "CreatedDate",
                        "is_deleted" => "IsDeleted",
                        "field" => "Field",
                        "value" => "NewValue",
                        "type" => "normalized",
                        "history_for" => "Opportunity"
                    }
                },
              "Opportunity" => {},
              "OpportunityLineItem" => {},
              "Account" => {},
              "OpportunityHistory" =>
              {
                  "custom" =>  {
                    "id" => "OpportunityId",
                    "timestamp" => "CreatedDate",
                    "is_deleted" => "IsDeleted",
                    "ignored" => ["SystemModstamp","CreatedById"],
                    "history_type" => "denormalized",
                    "history_for" => "Opportunity"
                  }
              },
              "Account" => {
                 "custom" => {
                     "id" => "OpportunityId",
                     "timestamp" => "CreatedDate",
                     "is_deleted" => "IsDeleted",
                     "ignored" => ["SystemModstamp","CreatedById"],
                     "type" => "denormalized",
                     "history_for" => "Account"
                 }
              }
          ]
        end

        def connect
          $log.info "Connecting to SalesForce"
          username = @metadata.get_configuration_by_type_and_key(@type,"username")
          password = @metadata.get_configuration_by_type_and_key(@type,"password")
          token = @metadata.get_configuration_by_type_and_key(@type,"token")
          client_id = @metadata.get_configuration_by_type_and_key(@type,"client_id")
          client_secret = @metadata.get_configuration_by_type_and_key(@type,"client_secret")
          oauth_refresh_token = @metadata.get_configuration_by_type_and_key(@type,"oauth_refresh_token")
          host = @metadata.get_configuration_by_type_and_key(@type,"host")
          version = @metadata.get_configuration_by_type_and_key(@type,"api_version") || DEFAULT_VERSION
          client_logger = @metadata.get_configuration_by_type_and_key(@type,"client_logger")

          if username && password && token
            credentials = {
                :username => username,
                :password => password,
                :security_token => token
            }
          elsif (oauth_refresh_token) && (!oauth_refresh_token.empty?)
            credentials = {
                :refresh_token => oauth_refresh_token
            }
          end

          if credentials
            credentials.merge!(
                :client_id => client_id,
                :client_secret => client_secret
            )
            credentials[:host] = host unless host.nil?
            credentials[:api_version] = version

            Restforce.log = true if client_logger

            @client = Restforce.new(credentials)
            @client.authenticate!

            @bulk_client = SalesforceBulkQuery::Api.new(@client, :logger => $log)

          end
        end

        def load_entities_metadata
          validation = @metadata.get_configuration_by_type_and_key(@type,"validation")
          @metadata.list_entities.each do |entity|
            pp entity.id
            description = @client.describe(entity.id)
            metadata_entity = entity
            temporary_entity = Metadata::Entity.new({"id" => metadata_entity.id, "name" => metadata_entity.name})
            if (!metadata_entity.disabled?)
              #metadata_entity["fields"] = []
              fields_in_source_system = []
              description.fields.each do |field|
                pp field["name"]
                type = nil
                case field["type"]
                  when "id"
                    type = "string-18"
                  when "boolean"
                    type = "boolean"
                  when "reference"
                    type = "string-18"
                  when "string","textarea","picklist","url","phone"
                    type = "string-#{field["length"]}"
                  when "currency","percent"
                    # This I am not sure
                    type = "decimal-#{field["precision"]}-#{field["scale"]}"
                  when "date"
                    type = "date-false"
                  when "double"
                    type = "decimal-#{field["precision"]}-#{field["scale"]}"
                  when "datetime"
                    type = "date-true"
                  when "int"
                    type = "integer"
                  else
                    $log.info "Unsupported salesforce type #{field["type"]} - using string(255) as default value"
                    type = "string-255"
                end
                field = Metadata::Field.new({
                    "id" => field["name"],
                    "name" => field["label"],
                    "type" => type,
                    "custom" => {}
                })
                temporary_entity.add_field(field)
              end
              # Merging entity and disabling add of new fields
              metadata_entity.merge!(temporary_entity,metadata_entity.custom["load_fields_from_source_system"])
              # if (!metadata_entity.custom.include?("validate") or metadata_entity.custom["validate"])
              #   folders = []
              #   folders << File.absolute_path(DEFAULT_VALIDATION_DIRECTORY)
              #   metadata_entity.generate_validations(folders,@type)
              # end
            end
          end
        end


        def download_entity_data(metadata_entity)
          single_batch = @metadata.get_configuration_by_type_and_key(@type,"single_batch")
          begin
            result = @bulk_client.query_fields(metadata_entity.id, {
                                        :directory_path => @data_directory,
                                        :created_from => Metadata::Runtime.get_entity_last_load(metadata_entity.id) || DEFAULT_MIN_DATE,
                                        :created_to => $now,
                                        :single_batch => single_batch,
                                        :fields => metadata_entity.get_enabled_fields,
                                        :timestamp => metadata_entity.custom["timestamp"]
            })

            metadata_entity.runtime["source_filename"] = merge_files(metadata_entity.id,result[:filenames])
            backup_to_bds(metadata_entity.runtime["source_filename"])
          rescue => e
            $log.warn "The download with Bulk API has failed, trying to download with normal API"
            query = construct_query(metadata_entity,metadata_entity.get_enabled_fields,Metadata::Runtime.get_entity_last_load(metadata_entity.id) || DEFAULT_MIN_DATE,$now,false,single_batch)
            $log.info "Executing query #{query}"
            data = @client.query(query)
            metadata_entity.runtime["source_filename"] = create_file(data,metadata_entity)
            backup_to_bds(metadata_entity.runtime["source_filename"])
          end
        end

        def download_entity_deleted_records(metadata_entity)
          fields = [metadata_entity.custom["id"],metadata_entity.custom["timestamp"],"IsDeleted"]
          soql = construct_query(metadata_entity,fields,Metadata::Runtime.get_entity_last_load(metadata_entity.id) || DEFAULT_MIN_DATE,$now,true)
          metadata_entity.runtime["source_deleted_filename"] = create_deleted_file(query_all(@client,soql),metadata_entity)
          backup_to_bds(metadata_entity.runtime["source_deleted_filename"])
        end

        def execute_validations(metadata_entity)
          metadata_entity.validations.each_pair do |key,types|
            types.each_pair do |type,validation|
              if (type == @type)
                values = {
                    "id" => metadata_entity.custom["id"],
                    "timestamap" => metadata_entity.custom["timestamp"],
                    "to" => $now,
                    "from" => Metadata::Runtime.get_entity_last_load(metadata_entity.id),
                    "entity_id" => metadata_entity.id
                }
                soql = Base::Templates.make_validation_template(validation,values)
                data = @client.query(soql)
                data.map do |row_hash|
                  validation.value = row_hash["expr0"]
                end
              end
            end
          end
        end


        def download_fields_for_additional_synchronization(metadata_entity)
          # In case that field is added to entity after the entity was already created, we need to resynchronize the last values
          single_batch = @metadata.get_configuration_by_type_and_key(@type,"single_batch")
          new_fields = metadata_entity.fields.values.find_all{|f| !f.disabled? and f.custom["synchronized"] == false }
          if (!Metadata::Runtime.get_entity_last_load(metadata_entity.id).nil? and !new_fields.empty? )
            begin
              result = @bulk_client.query_fields(metadata_entity.id, {
                  :directory_path => @data_directory,
                  :created_from => DEFAULT_MIN_DATE,
                  :created_to => $now,
                  :single_batch => single_batch,
                  :fields => new_fields.map {|v| v.id},
                  :timestamp => metadata_entity.custom["timestamp"]
              })

              metadata_entity.runtime["synchronization_source_filename"] = merge_files(metadata_entity.id,result[:filenames],"_synchronization")
              backup_to_bds(metadata_entity.runtime["synchronization_source_filename"])
            rescue => e
              $log.warn "The download with Bulk API has failed, trying to download with normal API"
              query = construct_query(metadata_entity,new_fields.map {|v| v.id},DEFAULT_MIN_DATE,$now,false)
              $log.info "Executing query #{query}"
              data = @client.query(query)
              metadata_entity.runtime["synchronization_source_filename"] = create_file(data,metadata_entity,"_synchronization")
              backup_to_bds(metadata_entity.runtime["synchronization_source_filename"])
            end
          end
        end


        def download_entity(metadata_entity)
          clean(metadata_entity)
          download_entity_data(metadata_entity)
          download_entity_deleted_records(metadata_entity)
          download_fields_for_additional_synchronization(metadata_entity)
          # execute_validations(metadata_entity)

          Metadata::Runtime.set_entity_last_load(metadata_entity.id,$now)
        end


        private


        def clean(metadata_entity)
          # Lets clean the runtime informations about the downloaded files
          metadata_entity.runtime.delete("synchronization_source_filename")
          metadata_entity.runtime.delete("source_deleted_filename")
          metadata_entity.runtime.delete("source_filename")
        end

        def query_all(client,soql)
          response = client.api_get 'queryAll', :q => soql
          response.body
        end

        def construct_query(metadata_entity,fields,from,to,deleted = false, full = false)
          # TODO - FIX INTERVAL
          raise Base::DownloaderException,"There are no fields to download for entity: #{metadata_entity.name} (#{metadata_entity.id})" if metadata_entity.get_enabled_fields.empty?
          base_query = ""
          base_query = "SELECT #{fields.join(', ')} FROM #{metadata_entity.id}"
          # if (deleted)
          #   base_query = "SELECT #{metadata_entity.custom["id"]},#{metadata_entity.custom["timestamp"]},IsDeleted FROM #{metadata_entity.id}"
          # else
          #   base_query = "SELECT #{metadata_entity.get_enabled_fields.join(', ')} FROM #{metadata_entity.id}"
          # end

          where = ""
          if (!full and deleted)
            where = " WHERE #{metadata_entity.custom["timestamp"]} >= #{from} AND #{metadata_entity.custom["timestamp"]} < #{to} and IsDeleted = true"
          elsif (!full)
            where = " WHERE #{metadata_entity.custom["timestamp"]} >= #{from} AND #{metadata_entity.custom["timestamp"]} < #{to}"
          elsif (deleted )
            where = " IsDeleted = true"
          end
          return base_query + where
        end


        def merge_files(entity,filenames,postfix = "")
          filenames.delete_if do |filename|
            delete = false
            File.open(filename) do |f|
              delete = true if f.readline == "Records not found for this query"
            end
            if delete
              FileUtils.rm(filename)
              true
            else
              false
            end
          end
          headers_written = false
          output_filename = "#{@data_directory}#{entity}#{postfix}.csv"
          FasterCSV.open(output_filename, 'w',:quote_char => '"',:force_quotes => true) do |csv|
            filenames.each do |filename|
              FasterCSV.foreach(filename, :headers => true,:quote_char => '"') do |csv_inner|
                if !headers_written
                  csv << csv_inner.headers
                  headers_written = true
                end
                csv << csv_inner
              end
              FileUtils.rm(filename)
            end
          end
          output_filename
        end


        def create_file(data,metadata_entity,postfix = "")
          output_filename = "#{@data_directory}#{metadata_entity.id}#{postfix}.csv"
          CSV.open(output_filename , 'w', :force_quotes => true) do |csv|
            # get the list of fields and write them as a header
            csv << metadata_entity.fields.values.map {|f| f.id}
            # write the stuff to the csv
            data.map do |row_hash|
              # get rid of the weird stuff coming from the api
              csv_line = row_hash.values_at(*metadata_entity.fields.values.map {|f| f.id}).map do |m|
                if m.kind_of?(Array)
                  m[0] == {"xsi:nil"=>"true"} ? nil : m[0]
                else
                  m
                end
              end
              csv << csv_line
            end
          end
          output_filename
        end

        def create_deleted_file(data,metadata_entity)
          output_filename = "#{@data_directory}#{metadata_entity.id}_deleted.csv"
          CSV.open(output_filename , 'w', :force_quotes => true) do |csv|
            # get the list of fields and write them as a header
            fields = [metadata_entity.custom["id"],metadata_entity.custom["timestamp"],"IsDeleted"]
            csv << fields
            # write the stuff to the csv
            data.map do |row_hash|
              # get rid of the weird stuff coming from the api
              csv_line = row_hash.values_at(*fields).map do |m|
                if m.kind_of?(Array)
                  m[0] == {"xsi:nil"=>"true"} ? nil : m[0]
                else
                  m
                end
              end
              csv << csv_line
            end
          end
          output_filename
        end

        #def download_deleted_record
        #  #OBSOLETE
        #  #request = "sobjects/Opportunity/deleted/?start=#{start_date}&end=#{end_date}"
        #  #request.gsub!("+","%2B")
        #  #pp @client.api_get(request).body
        #end



      end

    end
  end
end