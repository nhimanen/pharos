variable "region" {
  description = "Region for this module"
}

resource "aws_vpc" "this" {
  cidr_block = "10.0.0.0/16"
}
