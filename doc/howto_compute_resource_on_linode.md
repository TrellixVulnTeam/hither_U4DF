# How to set up a hither compute resource using linode

## Step 1: create a linode account and log in

See [linode.com](https://www.linode.com/)

## Step 2: Create a new Linode and log in

From the linode dashboard, create a new Linode (Linux server)

* Choose a distribution (recommended: Ubuntu 18.04)
* Select a Region (choose one that is geographically close to where you are)
* Select a Linode Plan
  * This depends on your compute needs (CPU, RAM, storage, network transfer)
* Type in a Linode Label, for example "hither-compute-resource-01"
* Choose a root password. I recommend using a secure password generator for this, and then storing it in a secure place.

Once it is up, log in. In a new terminal:

```bash
# find the ip address in the Networking tab for your Linode on the linode website
# use the password you selected above
ssh root@[ip]
```

## Step 3: Install prerequisites (docker, miniconda)

Inside your linode, we'll do everything as root

### Install docker

[Here is a nice guide that is specific to Ubuntu 18.04](https://www.digitalocean.com/community/tutorials/how-to-install-and-use-docker-on-ubuntu-18-04)

### Install miniconda

```bash
# Installing miniconda:
wget https://repo.anaconda.com/miniconda/Miniconda2-latest-Linux-x86_64.sh
bash Miniconda2-latest-Linux-x86_64.sh
# accept the license agreement
# use default location
# choose yes to initialize miniconda
# Then you must re-login
```

## Step 4. Install kachery and hither in a new conda env

### Set up the kachery storage directory

```bash
# Create a new directory for kachery storage
# and permanently set the KACHERY_STORAGE_DIR
# environment variable
mkdir /kachery-storage
echo "export KACHERY_STORAGE_DIR=/kachery-storage" >> ~/.bashrc
source ~/.bashrc
```

### Create a new conda environment with kachery and hither

```bash
# Create a new conda environment with kachery and hither
conda create --name hither python=3.8 numpy
conda activate hither

# Then inside the new conda env
conda install -c conda-forge nodejs
pip install --upgrade kachery_daemon kachery_client hither
```

For more information, see [the kacheryhub repo](https://github.com/kacheryhub) and [the hither repo](https://github.com/flatironinstitute/hither).

## Step 5. Start the kachery daemon

```bash
# Create a new tmux session
tmux new -s kachery-daemon

# Then within the new session, activate the conda env
conda activate hither

# Make sure that the kachery storage directory is set properly
echo $KACHERY_STORAGE_DIR

# Then start the kachery daemon
# For <LABEL> use an identifiable unique label
# For <OWNER> use an account with permissions on kachery-hub
# You can select a different port if you want
kachery-daemon start --label <LABEL> --owner <OWNER>
```

TODO: _Need to add link to specific kachery-hub documentation_

Once the daemon is running, you can detach from the tmux session via `ctrl+b d`

Later you can reconnect to the session via `tmux a -t kachery-daemon`

[See this guide for more information on using tmux](https://linuxize.com/post/getting-started-with-tmux/)

## Step 6. Configure and start the hither compute resource

### Configure the compute resource

```bash
# Create a new tmux session
tmux new -s compute-resource

# Then within the new session, activate the conda env
conda activate hither

# Make sure that the kachery storage directory is set properly
echo $KACHERY_STORAGE_DIR

# Create and cd to a compute-resource directory
mkdir compute-resource
cd compute-resource

# Configure the compute resource
hither-compute-resource config

# Answer the prompts
# When it asks to grant access to a kachery node, you can select the default "N" for now
```

### Start the compute resource

First, make sure you are in the compute-resource tmux session and the hither conda env. And also make sure that you are in the
compute-resource directory that you created above.

```bash
# Start the compute resource
hither-compute-resource start
```

You should see output that looks like the following:

```bash
Compute resource name: <name-you-chose>
Compute resource URI: <compute-resource-uri>

No kachery nodes have access to this compute resource

default: parallel job handler with <x> workers.
Starting compute resource: <name-you-chose>
```

Make a note of the compute resource URI. You will need that for running jobs.

Now you can detach from the tmux session as above `ctrl+b d`

### Give yourself permission to run jobs on this compute resource

In order to use your compute resource, you must explicitly give access to the kachery nodes that will be using your resource.

Let's imagine that you want to access this remote compute resource from your local workstation. First, be sure that the kachery
daemon is running on your workstation, and then determine your local kachery node id via:

```bash
# on your local workstation
kachery-daemon info
# Expected output:
# Node ID: <your-node-id>
```

Now copy that node ID. We are going to give permission to access the remote compute resource.

On the remote (linode) machine, stop the server and run config again:

```bash
# Attach to the compute-resource tmux session
# tmux a -t compute-resource
# kill the running compute resource server via Ctrl+C
# Then:
hither-compute-resource config

# When you get to the question "Would you like to grant access...", select "y" for yes
# Paste in the node ID from your local workstation
```

Now restart the compute resource server

```bash
hither-compute-resource start

# you should see your local node listed as having access
```

## Step 7: Run a test job

To test that the remote compute resource is configured properly, create the following two files on your local workstation:

```python
# add_one.py
import hither2 as hi

@hi.function('add_one', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def add_one(x):
    return x + 1
```

```python
# test_add_one.py

import hither2 as hi
from add_one import add_one

compute_resource_uri='feed://....' # paste compute resource uri from above
with hi.RemoteJobHandler(compute_resource_uri=compute_resource_uri) as jh:
    with hi.Config(
        job_handler=jh,
        container=True
    ):
        x = add_one.run(x=41).wait()
        print(f'Should be forty-two: {x}')
```

Now run:

```bash
python test_add_one.py
# The expected output is:
# Should be forty-two: 42
```

You should see activity on the remote compute resource indicating that the containerized computation was indeed performed remotely.

## Troubleshooting

Make sure that you are running a kachery daemon on both machines and that they belong to the same kachery channel

If something stops working, log in to the linode and check the two tmux sessions to verify that the kachery daemon and
the compute resource server are both still running. If one has crashed then you should make note of the console error
and then restart it. Please submit the issue to this repo.

## Using with labbox-ephys

In order to use a remote compute resource with labbox-ephys, you need to do two things:

* Give the labbox-ephys web server permission to run jobs on the compute resource. Follow the above procedure for
the node ID obtained in the configuration tab of the labbox-ephys GUI

* Configure labbox-ephys to use the new compute resource. See the devel/example_config_labbox.py script in the labbox-ephys repo.
You can modify and run that to generate a configuration URI (sha1://...) which you would then set in the kube config
file -- see `LABBOX_CONFIG_URI` in deployment.yml.

* **Important**: in order to do this, the large raw recordings must be on the computer where the compute resource is running.
We need to think about how this upload will be done.
