output "resource_group_name" {
  description = "Name of the provisioned Azure Resource Group"
  value       = azurerm_resource_group.this.name
}

output "storage_account_name" {
  description = "Name of the ADLS Gen2 Storage Account"
  value       = azurerm_storage_account.lakehouse.name
}

output "storage_account_primary_dfs_endpoint" {
  description = "Primary ADLS Gen2 DFS endpoint URL"
  value       = azurerm_storage_account.lakehouse.primary_dfs_endpoint
}

output "access_connector_id" {
  description = "Resource ID of the Databricks Access Connector for Unity Catalog"
  value       = azurerm_databricks_access_connector.unity_connector.id
}

output "access_connector_principal_id" {
  description = "Managed Identity Principal ID of the Access Connector"
  value       = azurerm_databricks_access_connector.unity_connector.identity[0].principal_id
}

output "key_vault_name" {
  description = "Name of the Azure Key Vault"
  value       = azurerm_key_vault.kv.name
}

output "databricks_workspace_url" {
  description = "URL of the Azure Databricks Workspace"
  value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "unity_catalog_name" {
  description = "Root Unity Catalog provisioned in Databricks"
  value       = databricks_catalog.lakehouse_catalog.name
}

output "bronze_schema_name" {
  description = "Full path of Bronze schema"
  value       = "${databricks_catalog.lakehouse_catalog.name}.${databricks_schema.bronze_schema.name}"
}

output "gold_schema_name" {
  description = "Full path of Gold schema"
  value       = "${databricks_catalog.lakehouse_catalog.name}.${databricks_schema.gold_schema.name}"
}
