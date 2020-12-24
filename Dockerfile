FROM amazon/aws-lambda-python:3
WORKDIR /var/task/
COPY . ./
RUN pip install -r requirements.txt
CMD [ "app.lambda_handler" ]