FROM golang:1.9

RUN apt-get update
RUN apt-get install python-dev python-pip python-setuptools build-essential -y

RUN pip install --upgrade setuptools
RUN pip install --upgrade csvkit

RUN mkdir -p /svc

COPY ./dist/getsample /svc

WORKDIR /svc

CMD ["bash"]
