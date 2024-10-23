// Initialize the provider
// The only configuration option needed is the API token

terraform {
  required_providers {
    aiven = {
      source  = "aiven/aiven"
      version = ">=4.2"
    }
  }
}

provider "aiven" {
  api_token = var.aiven_api_token
}
