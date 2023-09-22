# Instructions:
# Login to Azure CLI before running this script
# Run this script from the same terminal or command prompt where you logged in
# Set the variables below to match your needs

# If runing on windows - set this environment variable
export MSYS_NO_PATHCONV=1

# Troubleshooting
# If you get the error when running script such as "InvalidSchema: No connection adapters were found for 'C:/Program Files/Git/subscriptions/"
# then you need to set the environment variable MSYS_NO_PATHCONV=1 - displayed at top of script"

let "randomIdentifier=$RANDOM*$RANDOM"
echo $randomIdentifier

## Variables you need to set
service_location='canadaeast' # region where resources will be created
alias="serverless" # alias

resource_group_name="$alias-$randomIdentifier"

storage_acct_name=blobstore$randomIdentifier
abs_container_name_ingest='ingest'
abs_container_name_archive='archive'

adls_acct_name=datalake$randomIdentifier
fsys_name='filesystem'
dir_name='data'

key_vault_name=kv$randomIdentifier
abs_secret_name='abs-access-key1'
adls_secret_name='adls-access-key1'

funcapp_name=fn$randomIdentifier

# ############################################################################################
# Create an Azure Resource Group to organize the Azure services used in this series logically.
# ############################################################################################
az group create \
    --location $service_location \
    --name $resource_group_name

# ##########################################################################################
# Create a general-purpose storage account in your resource group and assign it an identity.
# ##########################################################################################
az storage account create \
    --name $storage_acct_name \
    --resource-group $resource_group_name \
    --location $service_location \
    --sku Standard_LRS \
    --assign-identity

# Create a storage container in a storage account.
az storage container create \
    --name $abs_container_name_ingest \
    --account-name $storage_acct_name \
    --auth-mode login

az storage container create \
    --name $abs_container_name_archive \
    --account-name $storage_acct_name \
    --auth-mode login

storage_acct_id=$(az storage account show \
                    --name $storage_acct_name  \
                    --resource-group $resource_group_name \
                    --query 'id' \
                    --output tsv)

# Capture storage account access key1
storage_acct_key1=$(az storage account keys list \
                        --resource-group $resource_group_name \
                        --account-name $storage_acct_name \
                        --query [0].value \
                        --output tsv)

# ###########################
# Create a ADLS Gen2 account.
# ###########################
az storage account create \
    --name $adls_acct_name \
    --resource-group $resource_group_name \
    --kind StorageV2 \
    --hns \
    --location $service_location \
    --sku Standard_LRS \
    --assign-identity

# Create a file system in ADLS Gen2
az storage fs create \
    --name $fsys_name \
    --account-name $adls_acct_name \
    --auth-mode login

# Create a directory in ADLS Gen2 file system
az storage fs directory create \
    --name $dir_name_bronze \
    --file-system $fsys_name \
    --account-name $adls_acct_name \
    --auth-mode login

az storage fs directory create \
    --name $dir_name_silver \
    --file-system $fsys_name \
    --account-name $adls_acct_name \
    --auth-mode login

az storage fs directory create \
    --name $dir_name_gold \
    --file-system $fsys_name \
    --account-name $adls_acct_name \
    --auth-mode login

adls_acct_key1=$(az storage account keys list \
                    --resource-group $resource_group_name \
                    --account-name $adls_acct_name \
                    --query [0].value
                    --output tsv)

# ####################################################
# Provision new Azure Key Vault in our resource group.
# ####################################################
az keyvault create  \
    --location $service_location \
    --name $key_vault_name \
    --resource-group $resource_group_name

# Create Secret for Azure Blob Storage Account
az keyvault secret set \
    --vault-name $key_vault_name \
    --name $abs_secret_name \
    --value $storage_acct_key1

# Create Secret for Azure Data Lake Storage Account
az keyvault secret set \
    --vault-name $key_vault_name \
    --name $adls_secret_name \
    --value $adls_acct_key1

# #######################################################
# Create a serverless function app in the resource group.
# #######################################################
az functionapp create \
    --name $funcapp_name \
    --storage-account $storage_acct_name \
    --consumption-plan-location $service_location \
    --resource-group $resource_group_name \
    --os-type Linux \
    --runtime python \
    --runtime-version 3.9 \
    --functions-version 4

az functionapp config appsettings set \
    --resource-group $resource_group_name \
    --name $funcapp_name \
    --settings "KEY_VAULT_RESOURCE_NAME=$key_vault_name" "KEY_VAULT_SECRET_NAME_ABS=$abs_secret_name" "KEY_VAULT_SECRET_NAME_ADLS=$adls_secret_name" "ABS_RESOURCE_NAME=$storage_acct_name" "ABS_CONTAINER_NAME_INGEST=$abs_container_name_ingest" "ABS_CONTAINER_NAME_ARCHIVE=$abs_container_name_archive" "ADLS_RESOURCE_NAME=$adls_acct_name" "ADLS_CONTAINER_NAME=$fsys_name" "ADLS_DIRECTORY_NAME_BRONZE=$dir_name_bronze" "ADLS_DIRECTORY_NAME_SILVER=$dir_name_silver" "ADLS_DIRECTORY_NAME_GOLD=$dir_name_gold" "AzureWebJobsFeatureFlags=EnableWorkerIndexing"

# Generate managed service identity for function app
az functionapp identity assign \
    --resource-group $resource_group_name \
    --name $funcapp_name

# Capture function app managed identity id
func_principal_id=$(az resource list \
            --name $funcapp_name \
            --query [*].identity.principalId \
            --output tsv)

# Capture key vault object/resource id
kv_scope=$(az resource list \
                --name $key_vault_name \
                --query [*].id \
                --output tsv)

# set permissions policy for function app to key vault - get list and set
az keyvault set-policy \
    --name $key_vault_name \
    --resource-group $resource_group_name \
    --object-id $func_principal_id \
    --secret-permission get list set

# Create a 'Key Vault Contributor' role assignment for function app managed identity
az role assignment create \
    --assignee $func_principal_id \
    --role 'Key Vault Contributor' \
    --scope $kv_scope

# Assign the 'Storage Blob Data Contributor' role to the function app managed identity
az role assignment create \
    --assignee $func_principal_id \
    --role 'Storage Blob Data Contributor' \
    --resource-group  $resource_group_name

# Assign the 'Storage Queue Data Contributor' role to the function app managed identity
az role assignment create \
    --assignee $func_principal_id \
    --role 'Storage Queue Data Contributor' \
    --resource-group  $resource_group_name