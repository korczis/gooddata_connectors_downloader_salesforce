module GoodData
  module Connectors
    module Downloader
      class SalesforceDownloader < GoodData::Connectors::Downloader::BaseDownloader

        attr_accessor :client,:bulk_client

        def initialize(metadata, params)
          @type = "salesforce"
          @default_api_version = '29.0'
          @format = "csv"

          super(metadata, params)

          # if the clients don't exist, create them
          if ! @global_params['salesforce_client']
            @global_params.merge!(GoodData::Bricks::RestForceMiddleware.create_client(@global_params))
          end
          if ! @global_params['salesforce_bulk_client']
            @global_params.merge!(GoodData::Bricks::BulkSalesforceMiddleware.create_client(@global_params))
          end

          # shortcuts for stuff obtained in params
          @client = @global_params['salesforce_client']
          @bulk_client = @global_params['salesforce_bulk_client']
        end

        def get_field_metadata
          @params['entities'].reduce({}) do |memo, obj|
            memo.merge({obj => get_fields(obj)})
          end
        end

        def download(field_metadata)
          downloaded_info = {
            'objects' => {},
            'meta' => {}
          }
          # take stuff from params
          objects = @params['entities']
          created_from = @params['created_from']
          created_to = @params['created_to']

          downloaded_info['meta']['salesforce_server'] = bulk_client.instance_url

          objects.each do |obj|
            name = generate_filename(obj)

            obj_fields = field_metadata[obj]

            user_defined_obj_fields = @params['limit_entity_fields'] ? @params['limit_entity_fields'][obj] : nil

            # if the fields were given by the user
            if user_defined_obj_fields
              user_defined_set = Set.new(user_defined_obj_fields)

              # if it contains some that aren't available, fail
              unavailable = user_defined_set - Set.new(obj_fields.map { |f| f['name'] })
              if ! unavailable.empty?
                raise "The following user defined fields for #{obj} aren't available: #{unavailable.map {|e| e}}"
              end

              # otherwise use only the given fields
              obj_fields = obj_fields.keep_if {|f| user_defined_set.member?(f['name'])}
            end

            main_data = download_main_dataset(obj, obj_fields)

            # if it's already in files, just write downloaded_info
            if main_data[:in_files]
              downloaded_info['objects'][obj] = {
                'fields' => obj_fields,
                'filenames' => main_data[:filenames].map {|f| File.absolute_path(f)}
              }
            else
              # otherwise write it to csv
              CSV.open(name, 'w', :force_quotes => true) do |csv|
                # get the list of fields and write them as a header
                csv << obj_fields.map {|f| f['name']}
                downloaded_info['objects'][obj] = {
                  'fields' => obj_fields,
                  'filenames' => [File.absolute_path(name)],
                }

                # write the stuff to the csv
                main_data[:data].map do |row_hash|
                  # get rid of the weird stuff coming from the api
                  csv_line = row_hash.values_at(*obj_fields.map {|f| f['name']}).map do |m|
                    if m.kind_of?(Array)
                      m[0] == {"xsi:nil"=>"true"} ? nil : m[0]
                    else
                      m
                    end
                  end
                  csv << csv_line
                end
              end
            end

          end
          return downloaded_info
        end

        private

        def download_main_dataset(obj, fields)
          created_from = @params["created_from"]
          created_to = @params["created_to"]
          single_batch = @params["single_batch"]

          q = construct_query(obj, fields)

          @logger.info "Executing soql: #{q}"

          begin
            filename_prefix = @params[:dss_table_prefix] ? @params[:dss_table_prefix] + '_' : nil

            # try it with the bulk
            result = @bulk_client.query(obj, q,
              :directory_path => @data_directory,
              :filename_prefix => filename_prefix,
              :created_from => created_from,
              :created_to => created_to,
              :single_batch => single_batch
            )

            return {
              :in_files => true,
              :filenames => result[:filenames]
            }
          rescue => e
            @logger.error "#{e.message}\n\n#{e.backtrace.join("\n")}"
            @logger.warn "Batch download failed. Now downloading through REST api instead"
            # if not, try the normal api
            # recreate the query so that it contains the from and to dates
            q = construct_query(obj, fields, created_from, created_to)
            data = @client.query(q)
            return {
              :in_files => false,
              :data => data
            }
          end
        end

        # get the list of fields for an object
        def get_fields(obj)
          description = @client.describe(obj)
          # return the names of the fields
          # TODO: handle - convert sfdc types to some metadata storage special types
          description.fields.map {|f| {'name' => f.name, 'type' => f.type, 'human_name' => f.label}}
        end

        def construct_query(obj, fields, created_from=nil, created_to=nil)
          base_query = "SELECT #{fields.map {|f| f['name']}.join(', ')} FROM #{obj}"
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
      end
    end
  end
end