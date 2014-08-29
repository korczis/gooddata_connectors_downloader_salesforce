require 'spec_helper'

describe GoodData::Connectors::Downloader::SalesforceDownloader do
  describe "download" do
    context "when given some basic params" do
      it "downloads something" do
        # configure it
        entities = [
          "User",
          "Opportunity",
          "OpportunityStage",
          "OpportunityHistory"
        ]
        params = {
          'config' => {
            'downloader' => {
              'salesforce' => {
                "username" => ENV["username"],
                "password" => ENV["password"],
                "token" => ENV["token"],
                "client_id" => ENV['client_id'],
                'client_secret' => ENV['client_secret'],
                'aws_access_key_id' => ENV['aws_access_key_id'],
                'aws_secret_access_key' => ENV['aws_secret_access_key'],
                's3_backup_bucket_name' => ENV['s3_backup_bucket_name'],
                "single_batch" => true,
                "entities" => entities
              }
            }
          }
        }
        downloader = GoodData::Connectors::Downloader::SalesforceDownloader.new(nil, params)
        # run it
        result = downloader.run

        # server should be there
        result['meta']['salesforce_server'].should_not be_empty

        # some objects
        result['objects'].length.should be(4)

        entities.each do |ent|
          # some fields for opp.
          result['objects'][ent]['fields'].should_not be_empty

          # some files with some content
          result['objects'][ent]['filenames'].length.should be(1)

          filename = result['objects'][ent]['filenames'][0]
          user_content = CSV.read(filename)

          # the metadata should be consistent with file contents
          result['objects'][ent]['fields'].length.should eq(user_content[0].length)
        end
      end
    end

  end
end
