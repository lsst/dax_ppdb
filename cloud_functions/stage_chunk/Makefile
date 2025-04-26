help:
	@echo "Usage:"
	@echo "  make container         Build and push the container to GCR"
	@echo "  make flex-template     Build the Flex template"
	@echo "  make function          Deploy the function"
	@echo "  make deploy            Redeploy the container, function, and Flex template"
	@echo "  make teardown          Tear down the function"
	@echo "  make logs              Show the last 50 logs for the function"
	@echo "  make help              Show this help message"

container:
	@echo "Building container..."
	./build-container.sh
	@echo "Done building container."

flex-template:
	@echo "Building flex template..."
	./build-flex-template.sh
	@echo "Done building Flex template."

function:
	@echo "Deploying function..."
	./deploy-function.sh
	@echo "Done deploying function."

deploy: container flex-template function

teardown:
	@echo "Tearing down function..."
	./teardown.sh
	@echo "Done tearing down function."

logs:
	gcloud functions logs read trigger_stage_chunk --region=us-central1 --limit=50 | less