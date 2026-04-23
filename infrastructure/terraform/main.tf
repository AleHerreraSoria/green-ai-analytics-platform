# --- IDENTIFICADOR ÚNICO ---
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# --- CAPAS S3 (ESTRUCTURA PERSISTENTE) ---
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

# --- GESTIÓN DEL EQUIPO (IAM) ---
locals {
  team_members = ["adrian-velazquez", "eduardo-cardenas", "jose-frias", "nuri-naranjo"]
}

resource "aws_iam_user" "team" {
  for_each      = toset(local.team_members)
  name          = each.value
  force_destroy = true
}

resource "aws_iam_user_login_profile" "team_login" {
  for_each        = aws_iam_user.team
  user            = each.value.name
  password_length = 12
}

resource "aws_iam_group" "data_engineers" {
  name = "DataEngineersGroup"
}

resource "aws_iam_group_policy_attachment" "admin_attach" {
  group      = aws_iam_group.data_engineers.name
  policy_arn = "arn:aws:iam::aws:policy/AdministratorAccess"
}

resource "aws_iam_user_group_membership" "team_membership" {
  for_each = aws_iam_user.team
  user     = each.value.name
  groups   = [aws_iam_group.data_engineers.name]
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

# --- NUEVOS SERVIDORES (CÓMPUTO) ---

resource "aws_instance" "airflow_server" {
  ami           = "ami-0c7217cdde317cfec"
  instance_type = "m7i-flex.large"
  key_name      = "green-ai-final-key" 

  vpc_security_group_ids = [aws_security_group.green_ai_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = { Name = "GreenAI-Airflow-Orchestrator" }
}

resource "aws_instance" "spark_server" {
  ami           = "ami-0c7217cdde317cfec"
  instance_type = "m7i-flex.large"
  key_name      = "green-ai-final-key"

  vpc_security_group_ids = [aws_security_group.green_ai_sg.id]

  root_block_device {
    volume_size = 20
    volume_type = "gp3"
    encrypted   = true
  }

  tags = { Name = "GreenAI-Spark-Processor" }
}

resource "aws_eip" "airflow_eip" {
  instance = aws_instance.airflow_server.id
  domain   = "vpc"
}

resource "aws_eip" "spark_eip" {
  instance = aws_instance.spark_server.id
  domain   = "vpc"
}

# --- OUTPUTS ---
output "bronze_bucket_name" { value = aws_s3_bucket.bronze_layer.bucket }
output "silver_bucket_name" { value = aws_s3_bucket.silver_layer.bucket }
output "gold_bucket_name"   { value = aws_s3_bucket.gold_layer.bucket }
output "airflow_public_ip"  { value = aws_eip.airflow_eip.public_ip }
output "spark_public_ip"    { value = aws_eip.spark_eip.public_ip }

output "team_passwords" {
  value     = { for u, p in aws_iam_user_login_profile.team_login : u => p.password }
  sensitive = true # Obligatorio para que Terraform acepte el plan
}