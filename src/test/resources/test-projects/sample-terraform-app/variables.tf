variable "region" {
  description = "AWS region to deploy into"
  default     = "us-east-1"
}

variable "instance_type" {
  default = "t2.micro"
}
