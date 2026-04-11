# Random ID para que los nombres de los buckets sean únicos globalmente
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# --- CAPAS S3 ---
resource "aws_s3_bucket" "bronze_layer" {
  bucket        = "${var.project_name}-bronze-${random_id.bucket_suffix.hex}"
  force_destroy = true 
}

resource "aws_s3_bucket" "silver_layer" {
  bucket        = "${var.project_name}-silver-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket" "gold_layer" {
  bucket        = "${var.project_name}-gold-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

# --- SEGURIDAD (Firewall) ---
resource "aws_security_group" "green_ai_sg" {
  name        = "green-ai-platform-sg"
  description = "Permitir SSH y acceso a Airflow"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

# --- SERVIDOR EC2 ---
resource "aws_instance" "platform_server" {
  ami           = "ami-0c7217cdde317cfec" 
  instance_type = "m7i-flex.large"
  key_name      = "green-ai-key"

  vpc_security_group_ids = [aws_security_group.green_ai_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = {
    Name = "GreenAI-Power-Server"
  }
}

# --- IP ESTÁTICA (Elastic IP) ---
resource "aws_eip" "platform_eip" {
  instance = aws_instance.platform_server.id
  domain   = "vpc"
  tags = { Name = "GreenAI-Static-IP" }
}

# --- OUTPUTS ---
output "bronze_bucket_name" { value = aws_s3_bucket.bronze_layer.bucket }
output "silver_bucket_name" { value = aws_s3_bucket.silver_layer.bucket }
output "gold_bucket_name" { value = aws_s3_bucket.gold_layer.bucket }
output "static_server_ip" { value = aws_eip.platform_eip.public_ip }