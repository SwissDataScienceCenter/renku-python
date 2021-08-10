#!/bin/bash -l
{% for option in sbatch_options.split() -%}
#SBATCH --{{ option }}
{% endfor %}

# Note: RENKU_TOKEN should be added during runtime as an evironment variable

RENKU_IMAGE={{ sbatch_image }}
RENKU_REMOTE={{ git_remote }}

# Environment variables to be used inside the container
export SINGULARITYENV_RENKU_PROJ_GIT=`echo $RENKU_REMOTE | sed "s/https:\/\//&oauth2:$RENKU_TOKEN@/g"`
export SINGULARITYENV_RENKU_PROJ_NAME=`echo $RENKU_REMOTE | sed "s/.*\/\(.*\)\.git$/\1/"`
export SINGULARITYENV_GIT_AUTHOR_NAME={{ git_username }}
export SINGULARITYENV_EMAIL={{ git_email }}

# Login infomation for RenkuLab reigistry
export SINGULARITY_DOCKER_USERNAME={{ git_username }}
export SINGULARITY_DOCKER_PASSWORD=$RENKU_TOKEN

# Bind paths
export SINGULARITY_BIND="${HOME}/runs,${SCRATCH}"

cd ${SCRATCH}

mkdir -p ${HOME}/runs/

cat << END > ${HOME}/runs/${SLURM_JOB_ID}-run_HPC.sh
#!/bin/bash

# overwrite HOME & PATH
HOME=/home/\${USER}
PATH=\${PATH}:\${HOME}/.local/bin

# check env
which renku

# clone the project
cd \${SCRATCH}
rm -rf \${RENKU_PROJ_NAME}_cluster
renku clone \${RENKU_PROJ_GIT} \${RENKU_PROJ_NAME}_cluster
cd \${RENKU_PROJ_NAME}_cluster

# renku command
{{ renku_command }}
renku save

# clean up
cd ..
rm -rf \${RENKU_PROJ_NAME}_cluster

END

# -c, --contain: use minimal /dev and empty other directories (e.g. /tmp and $HOME)
srun singularity run -c docker://$RENKU_IMAGE bash ${HOME}/runs/${SLURM_JOB_ID}-run_HPC.sh