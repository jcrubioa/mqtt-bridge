FROM python:3

ADD . /
WORKDIR /
RUN pip install -r requirements.txt
CMD [ "python", "./bridge.py" ]

