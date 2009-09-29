require 'aws/s3'

module BackupFuStores
  class AwsS3Store
    def initialize(conf)
      @fu_conf = conf
      
      unless AWS::S3::Base.connected?
        AWS::S3::Base.establish_connection!(
          :access_key_id => @fu_conf[:aws_access_key_id],
          :secret_access_key => @fu_conf[:aws_secret_access_key])
      end
      raise S3ConnectError, "\nERROR: Connection to Amazon S3 failed." unless AWS::S3::Base.connected?
    end
    
    def store_file(file)
      AWS::S3::S3Object.store(File.basename(file), open(file), @fu_conf[:s3_bucket], :access => :private)
    end
    
    def list_backups
      AWS::S3::Bucket.find(@fu_conf[:s3_bucket]).objects.map{|o| o.key.to_s }
    end
  end
end