# Aura_infrastructure
Ansible/Docker project for operating Aura's servers

## How to contribute
Please refer to [How To Contribute](https://github.com/Aura-healthcare/Aura_infrastructure/blob/master/CONTRIBUTING.md)

## tl;dr
Create the `group_vars/all/secrets.yml` file with the following variables:
```
jupyter_client_id: <GitHub OAuth application Client ID for JupyterHub>
jupyter_client_secret: <GitHub OAuth application Client Secret for JupyterHub>
```

`ansible-playbook -vv --diff -i inventories/prod.yml install.yml [-t time_series_db -t jupyter -t reverse_proxy]`

### Prerequisites
 * [Ansible v2.4.2](https://www.ansible.com/)
 * The server has to have a user `aura` with following `/etc/sudoers` configuration : `aura  ALL=(ALL)    NOPASSWD:  ALL`

## Development environment
### Prerequisites
 * [Vagrant v1.8.6](https://www.vagrantup.com/)
 * To have a public ssh key on your local machine on location : `~/.ssh/id_rsa.pub`
### tl;dr
 0. (only once) : `echo -e "192.168.33.22\tdb.aura.healthcare.local" | sudo tee -a /etc/hosts`
 1. `vagrant up`
 2. `ansible-playbook -i inventories/dev.yml install.yml [-t time_series_db -t reverse_proxy, ...]`
 3. Enjoy
 4. `vagrant destroy` 

### Usage
Your local vagrant environment is configured inside the inventory `inventories/dev.yml`.

You can run any playbook on this environment.

To have a local url that route to this development environment you can add this line in your hosts file (/etc/hosts) : `192.168.33.22   db.aura.healthcare.local`

Once you have executed the `vagrand up` command and run the `install.yml` playbook on the development environment you can change the configuration of the mobile app to use the `db.aura.healthcare.local` url, you can do any test.

You can ssh to the virtual machine with `ssh ansible@192.168.33.22` 
