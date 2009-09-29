require 'right_aws'

module BackupFuStores
  class RightAwsStore
    def initialize(conf)
      @fu_conf = conf
    end
  
    def s3
      @s3 ||= RightAws::S3.new(@fu_conf[:aws_access_key_id],
                               @fu_conf[:aws_secret_access_key])
    end
  
    def s3_bucket
      @s3_bucket ||= s3.bucket(@fu_conf[:s3_bucket], true, 'private')
    end
  
    def store_file(file)
      key = s3_bucket.key(File.basename(file))
      key.data = open(file)
      key.put(nil, 'private')
    end
  
    def s3_connection
      @s3 ||= begin
        RightAws::S3.new(@fu_conf[:aws_access_key_id], @fu_conf[:aws_secret_access_key])
      end
    end
    
    def list_backups
      s3_connection.bucket(@fu_conf[:s3_bucket]).keys.map(&:to_s)
    end
  end
end