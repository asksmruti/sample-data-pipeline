timestamp := `date "+%Y-%m-%d@%H:%M:%S"`
#export TF_LOG=DEBUG
init:
	rm -rf .terraform && terraform fmt -recursive && \
	terraform init

apply:
	terraform fmt -recursive && terraform apply

plan:
	terraform fmt -recursive && terraform plan

destroy:
	terraform fmt -recursive && terraform destroy

backup-state:
	terraform state pull > backup-states/backup-state-${timestamp}.json

restore-state:
	terraform state push -lock=true backup-states/backup-state-${timestamp}.json