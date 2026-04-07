terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # Configuración del Remote Backend Ultra PRO
  backend "s3" {
    bucket         = "green-ai-terraform-state-0e96d06"
    key            = "pf-dept02/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "terraform-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = "us-east-1"
  default_tags {
    tags = {
      Project     = "GreenAI"
      Environment = "Dev"
      ManagedBy   = "Terraform"
    }
  }
}