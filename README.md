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
