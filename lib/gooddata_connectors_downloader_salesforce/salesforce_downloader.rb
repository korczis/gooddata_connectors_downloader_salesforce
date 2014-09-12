module GoodData
  module Connectors
    module DownloaderSalesforce

      class SalesForceDownloader < Base::BaseDownloader
        DEFAULT_VERSION = '29.0'

        attr_accessor :client,:bulk_client

        def initialize(metadata,options = {})
          @type = "salesforce_downloader"
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

            @bulk_client = SalesforceBulkQuery::Api.new(client, :logger => $log)

          end
        end

        def load_entities_metadata
          @metadata.list_entities.each do |entity|
            description = @client.describe(entity.id)
            metadata_entity = entity
            temporary_entity = Metadata::Entity.new({"id" => metadata_entity.id, "name" => metadata_entity.name})
            if (!metadata_entity.disabled?)
              #metadata_entity["fields"] = []
              fields_in_source_system = []
              description.fields.each do |field|
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
                    "type" => type
                })
                temporary_entity.add_field(field)
              end
              # Merging entity and disabling add of new fields
              metadata_entity.merge!(temporary_entity,metadata_entity.custom["load_fields_from_source_system"])
            end
          end
        end


        def download_entity(metadata_entity)

          #metadata_entity = @metadata.get_entity(entity)
          created_from = @metadata.get_configuration_by_type_and_key(@type,"created_from")
          created_to = @metadata.get_configuration_by_type_and_key(@type,"created_to")
          single_batch = @metadata.get_configuration_by_type_and_key(@type,"single_batch")

          query = construct_query(metadata_entity)
          $log.info "Executing query #{query}"
          begin
            result = @bulk_client.query(metadata_entity.id,query,
                                            :directory_path => @data_directory,
                                            :created_from => created_from,
                                            :created_to => created_to,
                                            :single_batch => single_batch
            )
            metadata_entity.runtime["source_filename"] = merge_files(metadata_entity.id,result[:filenames])
            backup_to_bds(metadata_entity,metadata_entity.runtime["source_filename"])
          rescue => e
            $log.warn "The download with Bulk API has failed, trying to download with normal API"
            query = construct_query(metadata_entity,created_from,created_to)
            $log.info "Executing query #{query}"
            data = @client.query(query)
            metadata_entity.runtime["source_filename"] = create_file(data,metadata_entity)
            backup_to_bds(metadata_entity,metadata_entity.runtime["source_filename"])
          end
        end


        private

        def construct_query(metadata_entity, created_from=nil, created_to=nil)
          # TODO - FIX INTERVAL
          raise Base::DownloaderException,"There are no fields to download for entity: #{metadata_entity.name} (#{metadata_entity.id})" if metadata_entity.get_enabled_fields.empty?
          base_query = "SELECT #{metadata_entity.get_enabled_fields.join(', ')} FROM #{metadata_entity.id}"
          if created_from && created_to
            return base_query + " WHERE CreatedDate >= #{created_from} AND CreatedDate < #{created_to}"
          end
          if created_from
            return base_query + " WHERE CreatedDate >= #{created_from}"
          end
          if created_to
            return base_query + " WHERE CreatedDate < #{created_to}"
          end
          return base_query
        end


        def merge_files(entity,filenames)
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
          output_filename = "#{@data_directory}#{entity}.csv"
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


        def create_file(data,metadata_entity)
          output_filename = "#{@data_directory}#{metadata_entity.id}.csv"
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

      end

    end
  end
end