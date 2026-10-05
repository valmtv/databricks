# -----------------------------------------------------------------------------
# Terraform: Azure Infrastructure & Databricks Lakehouse Foundation
# Project: Air Quality Medallion Lakehouse (Lab 8 CI/CD & Infrastructure as Code)
# -----------------------------------------------------------------------------

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  name_prefix       = "${var.project_name}-${var.environment}"
  resource_group    = var.resource_group_name != "" ? var.resource_group_name : "rg-${local.name_prefix}-${var.azure_region}"
  storage_account   = var.storage_account_name != "" ? var.storage_account_name : substr("st${var.project_name}${var.environment}${random_string.suffix.result}", 0, 24)
  workspace_name    = var.databricks_workspace_name != "" ? var.databricks_workspace_name : "dbw-${local.name_prefix}"
  key_vault_name    = substr("kv-${var.project_name}-${var.environment}-${random_string.suffix.result}", 0, 24)
}

# -----------------------------------------------------------------------------
# 1. Resource Group
# -----------------------------------------------------------------------------
resource "azurerm_resource_group" "this" {
  name     = local.resource_group
  location = var.azure_region
  tags     = var.tags
}

# -----------------------------------------------------------------------------
# 2. ADLS Gen2 Storage Account (Hierarchical Namespace Enabled for Lakehouse)
# -----------------------------------------------------------------------------
resource "azurerm_storage_account" "lakehouse" {
  name                     = local.storage_account
  resource_group_name      = azurerm_resource_group.this.name
  location                 = azurerm_resource_group.this.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # Critical: Enables ADLS Gen2 directory hierarchy

  min_tls_version                 = "TLS1_2"
  enable_https_traffic_only       = true
  allow_nested_items_to_be_public = false

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  tags = var.tags
}

# Lakehouse Root Filesystem Container (Unity Catalog External Storage)
resource "azurerm_storage_data_lake_gen2_filesystem" "unity_catalog_root" {
  name               = "unity-catalog-root"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

# Raw Telemetry Landing Container
resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "air-quality-landing"
  storage_account_id = azurerm_storage_account.lakehouse.id
}

# -----------------------------------------------------------------------------
# 3. Azure Databricks Access Connector (Managed Identity for Unity Catalog)
# -----------------------------------------------------------------------------
resource "azurerm_databricks_access_connector" "unity_connector" {
  name                = "dbac-${local.name_prefix}"
  resource_group_name = azurerm_resource_group.this.name
  location            = azurerm_resource_group.this.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Grant Access Connector "Storage Blob Data Contributor" role on the ADLS Gen2 Account
resource "azurerm_role_assignment" "access_connector_blob_contributor" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity_connector.identity[0].principal_id
}

# -----------------------------------------------------------------------------
# 4. Azure Key Vault (Secrets Management for CI/CD & Workspace Credentials)
# -----------------------------------------------------------------------------
resource "azurerm_key_vault" "kv" {
  name                        = local.key_vault_name
  location                    = azurerm_resource_group.this.location
  resource_group_name         = azurerm_resource_group.this.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get", "List", "Create", "Delete", "Update"
    ]

    secret_permissions = [
      "Get", "List", "Set", "Delete", "Purge", "Recover"
    ]
  }

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 5. Azure Databricks Workspace (Premium Tier for Unity Catalog Support)
# -----------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "this" {
  name                        = local.workspace_name
  resource_group_name         = azurerm_resource_group.this.name
  location                    = azurerm_resource_group.this.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "mrg-${local.name_prefix}"

  tags = var.tags
}

# -----------------------------------------------------------------------------
# 6. Databricks Unity Catalog Objects (Declarative Data Governance)
# -----------------------------------------------------------------------------
# Storage Credential referencing the Azure Access Connector Managed Identity
resource "databricks_storage_credential" "external_creds" {
  name = "cred_${var.project_name}_${var.environment}"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity_connector.id
  }
  comment = "Managed Identity storage credential for Air Quality ADLS Gen2"
  depends_on = [
    azurerm_role_assignment.access_connector_blob_contributor,
    azurerm_databricks_workspace.this
  ]
}

# External Location pointing to ADLS Gen2 Root Container
resource "databricks_external_location" "lakehouse_external_location" {
  name            = "extloc_${var.project_name}_${var.environment}"
  url             = "abfss://${azurerm_storage_data_lake_gen2_filesystem.unity_catalog_root.name}@${azurerm_storage_account.lakehouse.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.external_creds.id
  comment         = "External location for Air Quality Medallion Lakehouse root tables"
}

# Unity Catalog Catalog
resource "databricks_catalog" "lakehouse_catalog" {
  name          = var.unity_catalog_name
  storage_root  = "abfss://${azurerm_storage_data_lake_gen2_filesystem.unity_catalog_root.name}@${azurerm_storage_account.lakehouse.name}.dfs.core.windows.net/catalogs/${var.unity_catalog_name}"
  comment       = "Production catalog for Air Quality Medallion Architecture"
  force_destroy = false
  depends_on    = [databricks_external_location.lakehouse_external_location]
}

# Bronze Schema for Ingestion
resource "databricks_schema" "bronze_schema" {
  catalog_name  = databricks_catalog.lakehouse_catalog.id
  name          = var.user_bronze_schema
  comment       = "Bronze layer for streaming & batch air quality raw landing data"
  force_destroy = false
}

# Gold Schema for Analytics & BI Presentation
resource "databricks_schema" "gold_schema" {
  catalog_name  = databricks_catalog.lakehouse_catalog.id
  name          = var.user_gold_schema
  comment       = "Gold star-schema layer for facts, dimensions, and executive dashboards"
  force_destroy = false
}

# Unity Catalog Volume for File Landing (CSV / JSON telemetry drops)
resource "databricks_volume" "landing_volume" {
  name             = "air_quality_landing"
  catalog_name     = databricks_catalog.lakehouse_catalog.id
  schema_name      = databricks_schema.bronze_schema.name
  volume_type      = "MANAGED"
  comment          = "Managed volume for landing incoming sensor telemetry payloads"
}
