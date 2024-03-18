ARG BUILD_FROM
FROM $BUILD_FROM

WORKDIR /app

# Install requirements for add-on
# (alpine image)
RUN apk add python3 py-pip
#RUN apk add --no-cache bluez
#RUN apk add --no-cache git

COPY . .

RUN python3 -m venv venv
RUN venv/bin/pip3 install -r requirements.txt

# RUN . venv/bin/activate
# RUN chmod a+x run.sh

CMD [ "venv/bin/python3", "main.py" ]
