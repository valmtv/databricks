variable "environment" {
  description = "Deployment environment name (e.g., prod, dev, staging)"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project identifier used in resource naming prefixes"
  type        = string
  default     = "airquality"
}

variable "azure_region" {
  description = "Azure region where resources will be provisioned"
  type        = string
  default     = "westeurope"
}

variable "resource_group_name" {
  description = "Name of the Azure Resource Group (leave blank to auto-generate from project & environment)"
  type        = string
  default     = ""
}

variable "storage_account_name" {
  description = "Name of the ADLS Gen2 Storage Account (3-24 lowercase alphanumeric chars; leave blank to auto-generate)"
  type        = string
  default     = ""
}

variable "databricks_workspace_name" {
  description = "Name of the Azure Databricks Workspace (leave blank to auto-generate)"
  type        = string
  default     = ""
}

variable "databricks_sku" {
  description = "SKU pricing tier for Databricks (standard or premium). Unity Catalog requires 'premium'."
  type        = string
  default     = "premium"
}

variable "unity_catalog_name" {
  description = "Name of the Unity Catalog root catalog for production lakehouse"
  type        = string
  default     = "dbr_dev"
}

variable "user_bronze_schema" {
  description = "Bronze schema name for ingested raw telemetry"
  type        = string
  default     = "valeriimatviiv_bronze"
}

variable "user_gold_schema" {
  description = "Gold schema name for star-schema dimensional models"
  type        = string
  default     = "valeriimatviiv_gold"
}

variable "tags" {
  description = "Tags applied to all provisioned Azure resources for cost tracking and governance"
  type        = map(string)
  default = {
    Owner       = "Valerii Matviiv"
    Environment = "prod"
    ManagedBy   = "Terraform"
    Project     = "Air Quality Medallion Lakehouse"
    Internship  = "Lab 8-9 CI/CD Platform Automation"
  }
}
