FROM renku/singleuser:latest

# Uncomment and adapt if code is to be included in the image
# COPY src /code/src

# install the python dependencies
COPY requirements.txt /tmp/requirements.txt
USER 0
RUN pip install -r /tmp/requirements.txt

# switch to the user that will be used in the notebook
USER 1000
