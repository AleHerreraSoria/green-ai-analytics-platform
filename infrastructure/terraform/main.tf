# Random ID para que los nombres de los buckets sean únicos globalmente
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Capa BRONZE (Raw Data)
resource "aws_s3_bucket" "bronze_layer" {
  bucket        = "${var.project_name}-bronze-${random_id.bucket_suffix.hex}"
  force_destroy = true # IMPORTANTE: Permite borrar el bucket aunque tenga archivos (ideal para PF)
}

# Capa SILVER (Cleansed Data)
resource "aws_s3_bucket" "silver_layer" {
  bucket        = "${var.project_name}-silver-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# Capa GOLD (Curated Data)
resource "aws_s3_bucket" "gold_layer" {
  bucket        = "${var.project_name}-gold-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# Outputs para que el equipo sepa qué nombres se generaron
output "bronze_bucket_name" {
  value = aws_s3_bucket.bronze_layer.bucket
}
output "silver_bucket_name" {
  value = aws_s3_bucket.silver_layer.bucket
}
output "gold_bucket_name" {
  value = aws_s3_bucket.gold_layer.bucket
}