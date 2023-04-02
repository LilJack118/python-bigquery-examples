FROM python:3.11.1-alpine3.16

# it tells docker to prevent buffer python (logs will be printed in the console)
ENV PYTHONBUFFERED 1

COPY ./requirements.txt /tmp/requirements.txt
COPY ./requirements.dev.txt /tmp/requirements.dev.txt
COPY ./ /src

WORKDIR /src

ARG DEV=false
RUN python -m venv /py && \
    /py/bin/pip install --upgrade pip && \
    apk add --no-cache python3-dev libffi-dev gcc musl-dev make && \
    apk add --update --no-cache build-base musl-dev && \
    apk add --update --no-cache --virtual .tmp-build-deps \
    /py/bin/pip install -r /tmp/requirements.txt && \
    # if DEV arg is set to true install requirements.dev
    if [ $DEV = "true" ]; \
    then /py/bin/pip install -r /tmp/requirements.dev.txt; \
    fi && \
    rm -rf /tmp && \
    apk del \
    .tmp-build-deps && \
    # add user to docker image (to don't have to use root user)
    adduser \
    --disabled-password \
    --no-create-home \
    dev-user

# update path environment variable (to run python command automatically
# from virtual env)
ENV PATH="/py/bin:$PATH"
# specify user that we switching to (before that we used root user)
USER dev-user