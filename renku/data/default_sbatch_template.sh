#!/bin/bash -l
{% for option in sbatch_options.split() -%}
#SBATCH --{{ option }}
{% endfor %}

# Note: RENKU_TOKEN should be added during runtime as an evironment variable

export RENKU_IMAGE={{ sbatch_image }}
RENKU_REMOTE={{ git_remote }}
export RENKU_PROJ_GIT=`echo $RENKU_REMOTE | sed "s/https:\/\//&oauth2:$RENKU_TOKEN@/g"`
export RENKU_PROJ_NAME=`echo $RENKU_REMOTE | sed "s/.*\/\(.*\)\.git$/\1/"`

export GIT_AUTHOR_NAME={{ git_username }}
export EMAIL={{ git_email }}

export SINGULARITY_DOCKER_USERNAME=$GIT_AUTHOR_NAME
export SINGULARITY_DOCKER_PASSWORD=$RENKU_TOKEN

cd ${SCRATCH}

mkdir -p ${HOME}/runs/

cat << END > ${HOME}/runs/${SLURM_JOB_ID}-run_HPC.sh
#!/bin/bash
set -e

# overwrite HOME & PATH
HOME=/home/\${USER}
PATH=\${PATH}:\${HOME}/.local/bin

# check env
which python

# clone the project
cd \${SCRATCH}
rm -rf \${RENKU_PROJ_NAME}
renku clone \${RENKU_PROJ_GIT}
cd \${RENKU_PROJ_NAME}

# renku command
{{ renku_command }}
renku save

# clean up
cd ..
rm -rf \${RENKU_PROJ_NAME}

rm ${HOME}/runs/${SLURM_JOB_ID}-run_HPC.sh
END

export SINGULARITY_BIND="${HOME}/runs/,${SCRATCH}"
srun singularity run -C docker://${RENKU_IMAGE} bash ${HOME}/runs/${SLURM_JOB_ID}-run_HPC.sh