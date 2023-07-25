ARG BUILD_FROM
FROM $BUILD_FROM

WORKDIR /app

# Install requirements for add-on
# (alpine image)
RUN apk add python3 py-pip
#RUN apk add --no-cache bluez
#RUN apk add --no-cache git

COPY . .

RUN pip3 install -r requirements.txt
#RUN chmod a+x run.sh

CMD [ "/usr/bin/python3", "main.py" ]
