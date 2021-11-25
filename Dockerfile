FROM python:3
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
CMD [ "python", "./main.py" ]
