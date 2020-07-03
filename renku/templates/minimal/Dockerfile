FROM renku/renkulab-py:3.7-renku0.10.4-0.6.3
# see https://github.com/SwissDataScienceCenter/renkulab-docker
# to swap this image for the latest version available

# Uncomment and adapt if code is to be included in the image
# COPY src /code/src

# Uncomment and adapt if your R or python packages require extra linux (ubuntu) software
# e.g. the following installs apt-utils and vim; each pkg on its own line, all lines
# except for the last end with backslash '\' to continue the RUN line
#
# USER root
# RUN apt-get update && \
#    apt-get install -y --no-install-recommends \
#    apt-utils \
#    vim
# USER ${NB_USER}

# Uncomment and adapt if you want to automatically install
# python dependencies when the Docker image builds (pip or conda)
# Note: you will need to add a (pip) requirements.txt file
#       AND a (conda) environment.yml file for the below code to run,
#       but you can remove one or the other.
#
# COPY requirements.txt environment.yml /tmp/
# RUN conda env update -q -f /tmp/environment.yml && \
#    /opt/conda/bin/pip install -r /tmp/requirements.txt && \
#    conda clean -y --all && \
#    conda env export -n "root"
