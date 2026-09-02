data "aws_ami" "app" {
  most_recent = true
}

resource "aws_instance" "web" {
  ami           = data.aws_ami.app.id
  instance_type = var.instance_type
}

module "vpc" {
  source = "./modules/vpc"
  region = var.region
}

output "instance_id" {
  value = aws_instance.web.id
}
