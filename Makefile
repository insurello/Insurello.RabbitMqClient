PROJECT_NAME=Insurello.RabbitMqClient
PROJECT_DIR=src/$(PROJECT_NAME)
MAIN_PROJ=$(MAIN_DIR)/$(PROJECT_NAME).fsproj


.PHONY : all build

all: build

build:
	dotnet build

clean:
	rm -r $(PROJECT_DIR)/bin $(PROJECT_DIR)/obj 
