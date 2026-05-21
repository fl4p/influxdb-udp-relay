FROM ghcr.io/home-assistant/base:latest

WORKDIR /app

# Install requirements for add-on
# (alpine image)
RUN apk add python3 py-pip
#RUN apk add --no-cache bluez
#RUN apk add --no-cache git

COPY . .

RUN python3 -m venv venv
# tamp (binary-protocol decode) ships a C speedup but falls back to pure Python;
# transient build deps let pip compile it from sdist if no wheel exists for the arch.
RUN apk add --no-cache --virtual .build gcc musl-dev python3-dev \
 && venv/bin/pip3 install -r requirements.txt \
 && apk del .build

# RUN . venv/bin/activate
# RUN chmod a+x run.sh

CMD [ "venv/bin/python3", "main.py" ]
