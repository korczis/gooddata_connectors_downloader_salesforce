require "gooddata_connectors_base"
require "gooddata"
require 'restforce'
require 'salesforce_bulk_query'

require "gooddata_connectors_downloader_salesforce/version"
require "gooddata_connectors_downloader_salesforce/salesforce_downloader"

module GoodDataConnectorsDownloaderSalesforce


  class SalesForceDownloaderMiddleWare < GoodData::Bricks::Middleware

    def call(params)
      $log = params["GDC_LOGGER"]
      $log.info "Initializing SalesForceDownloaderMiddleware"
      salesforce_downloader = SalesForceDownloader.new(params["metadata_wrapper"],params)



      @app.call(params.merge('salesforce_downloader_wrapper' => salesforce_downloader))
      # Salesforce downloader specific things
    end



  end



end
