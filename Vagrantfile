# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "centos/7"

  config.vm.define "aura-vm" do |config|
    config.vm.host_name = "aura-vm"
    config.vm.network :private_network, ip: "192.168.33.22"
    config.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--cpus", 2]
      vb.memory = 4096
    end
  end
  config.vm.provision "shell", :privileged => true, inline: "useradd -m -s /bin/bash -U ansible"
  config.vm.provision "file", source: "~/.ssh/id_rsa.pub", destination: "/tmp/authorized.pub"
  config.vm.provision "shell", :privileged => true, inline: "mkdir /home/ansible/.ssh/; cat /tmp/authorized.pub >> /home/ansible/.ssh/authorized_keys"
  config.vm.provision "shell", :privileged => true, inline: "echo 'ansible ALL=(ALL) NOPASSWD:ALL' | sudo tee -a /etc/sudoers"
end
