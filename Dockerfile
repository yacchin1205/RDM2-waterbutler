FROM python:3.11-slim-trixie AS build

# Install dependancies
RUN apt-get update \
    && apt-get install -y \
        git \
        libevent-dev \
        libxml2-dev \
        libxslt1-dev \
        zlib1g-dev \
        # cryptography
        build-essential \
        libssl-dev \
        libffi-dev \
        gnupg2 \
    && apt-get clean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /code
WORKDIR /code

RUN pip install --upgrade pip setuptools==80.10.2

COPY ./requirements.txt /code/

RUN pip wheel --wheel-dir=/wheels -r /code/requirements.txt

# Copy the rest of the code over
COPY ./ /code/

RUN pip wheel --no-deps --wheel-dir=/wheels .

FROM python:3.11-slim-trixie

RUN usermod -d /home www-data \
    && chown www-data:www-data /home \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        # grab gosu for easy step-down from root
        gosu \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /code
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir --no-index --no-deps /wheels/*.whl \
    && pip check \
    && pip uninstall -y pip \
    && rm -rf /wheels
COPY --from=build /code /code

ARG GIT_COMMIT=
ENV GIT_COMMIT=${GIT_COMMIT}

RUN sed -i -e 's/CipherString = DEFAULT@SECLEVEL=2/CipherString = DEFAULT@SECLEVEL=1/g' /etc/ssl/openssl.cnf

EXPOSE 7777

CMD ["gosu", "www-data", "invoke", "server"]
