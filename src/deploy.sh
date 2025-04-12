#!/bin/bash

# Build the application
mvn clean package

# Copy to EC2
scp -i "<filename.pem>" target/movie-data-transformer-0.0.1-SNAPSHOT.jar \
    ec2-user@e<your_ec2_instance>:/home/ec2-user/app/

# Copy configuration
scp -i "<filename.pem>" src/main/resources/application.properties \
    ec2-user@<your_ec2_instance>:/home/ec2-user/app/config/



