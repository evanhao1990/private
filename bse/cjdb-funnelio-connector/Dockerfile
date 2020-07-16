
FROM python:3.7

RUN mkdir -p /code
WORKDIR /code
ADD ./requirements.txt /code
RUN pip install --no-cache-dir -r /code/requirements.txt
ADD ./ /code/