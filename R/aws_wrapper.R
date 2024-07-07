library(aws.s3)
bucketlist()
Sys.setenv(
  "AWS_ACCESS_KEY_ID" = "key",
  "AWS_SECRET_ACCESS_KEY" = "secret",
  "AWS_DEFAULT_REGION" = "us-east-2"
)

bucket <- "rirl-documents"
object <- "metadata/archivos/contratos/20-2020.pdf"
custom_path <- "your/custom/path/file.pdf"  # Replace with your desired local path
save_object(object = object, bucket = bucket, file = custom_path)
