#!/bin/bash
# Copyright (C) 2013-2017 Yiqun Zhang <contact@yzhang.io>
# All Rights Reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Settings
remotePackagePath="http://www2.cs.uh.edu/~yzhang/packages"
localPackagePath="$HOME/packages"
scidbSourcePack=scidb-16.9.0.db1a98f.tgz
shimPackageURL="https://github.com/Paradigm4/shim/raw/gh-pages/shim_16.9_amd64.deb"
shimPackageFileName=`echo "$shimPackageURL" | sed "s#.*\/\(.*\.deb$\)#\1#g"`
SCIDB_VER=16.9
SCIDB_SOURCE_PATH=$HOME/scidbtrunk
SCIDB_BUILD_PATH=$SCIDB_SOURCE_PATH/stage/build
SCIDB_INSTALL_PATH=/opt/scidb/$SCIDB_VER
SCIDB_BUILD_TYPE=RelWithDebInfo
scidbPackages=(
    scidb-16.9-all-coord_0-1_amd64.deb
    scidb-16.9-dev-tools_0-1_amd64.deb
    scidb-16.9-all_0-1_amd64.deb
    scidb-16.9-dev_0-1_all.deb
    scidb-16.9-client-dbg_0-1_amd64.deb
    scidb-16.9-jdbc_0-1_all.deb
    scidb-16.9-client-python-dbg_0-1_amd64.deb
    scidb-16.9-plugins-dbg_0-1_amd64.deb
    scidb-16.9-client-python_0-1_amd64.deb
    scidb-16.9-plugins_0-1_amd64.deb
    scidb-16.9-client_0-1_amd64.deb
    scidb-16.9-tests_0-1_all.deb
    scidb-16.9-dbg_0-1_amd64.deb
    scidb-16.9-utils-dbg_0-1_amd64.deb
    scidb-16.9-dbginfo_0-1_amd64.deb
    scidb-16.9-utils_0-1_amd64.deb
    scidb-16.9-dev-tools-dbg_0-1_amd64.deb
    scidb-16.9_0-1_amd64.deb
    $scidbSourcePack
)
prerequisites=(
    build-essential
    subversion
    expect
    openssh-server
    openssh-client
    gdebi
    git
    # The following two packages are the prerequisites for the R devtools
    libcurl4-openssl-dev
    libssl-dev
    r-base-core
)
scidbVarNames=(
    SCIDB_VER
    SCIDB_SOURCE_PATH
    SCIDB_BUILD_PATH
    SCIDB_INSTALL_PATH
    SCIDB_BUILD_TYPE
)
scidbVarValues=(
    $SCIDB_VER
    $SCIDB_SOURCE_PATH
    $SCIDB_BUILD_PATH
    $SCIDB_INSTALL_PATH
    $SCIDB_BUILD_TYPE
)

printLog() {
    if [ $# -eq 4 ]; then
        for i in `seq 1 1 $1`; do echo -n " "; done
        echo -e "$2[$3] \033[0m$4"
    fi
}

printInfo() {
    if [ $# -eq 2 ]; then printLog $1 "\033[1;96m" "INFO" "$2"; fi
}

printWarn() {
    if [ $# -eq 2 ]; then printLog $1 "\033[1;93m" "WARN" "$2"; fi
}

printError() {
    if [ $# -eq 2 ]; then printLog $1 "\033[1;91m" "ERROR" "$2"; fi
}

downloadSciDB() {
    printInfo 0 "Download SciDB packages to $localPackagePath:"
    mkdir -p $localPackagePath
    cd $localPackagePath
    for pkg in ${scidbPackages[@]}; do
        printInfo 4 "Downloading $pkg..."
        wget -q $remotePackagePath/$pkg
    done
}

unpackSciDBSource() {
    if ! [ -f $localPackagePath/$scidbSourcePack ]; then
        printError 0 "Cannot find SciDB source code file at $localPackagePath/$scidbSourcePack"
        exit -1
    fi
    printInfo 0 "Extracting SciDB source..."
    mkdir -p $SCIDB_SOURCE_PATH
    tar -xzf $localPackagePath/$scidbSourcePack -C $SCIDB_SOURCE_PATH
}

installPrerequisite() {
    printInfo 0 "Retrieving new lists of packages..."
    sudo apt-get update > /dev/null
    printInfo 0 "Install prerequisites:"
    for preq in ${prerequisites[@]}; do
        printInfo 4 "Installing $preq..."
        sudo apt-get install -y $preq > /dev/null
    done
}

configEnvVars() {
    printInfo 0 "Adding environment variables..."
    for varName in ${scidbVarNames[@]}; do
        sudo sed -i "/$varName=/d" $HOME/.bashrc
    done
    if [ ${#scidbVarNames[@]} -ne ${#scidbVarValues[@]} ]; then
        printError 0 "scidbVarNames and scidbVarValues cannot match, check the source code."
        exit -1;
    fi
    i=0
    while [ $i -lt ${#scidbVarNames[@]} ]; do
        sudo echo "export ${scidbVarNames[$i]}=${scidbVarValues[$i]}" >> $HOME/.bashrc
        let i=i+1
    done
    echo "export PATH=\"\$SCIDB_INSTALL_PATH/bin:.:\$PATH\"" >> $HOME/.bashrc
    echo "export LD_LIBRARY_PATH=\"\$SCIDB_INSTALL_PATH/lib\"" >> $HOME/.bashrc
    echo "export PATH=\".:\$PATH\"" >> $HOME/.bashrc
    echo "alias ..='cd ../'" >> $HOME/.bashrc
    echo "alias ...='cd ../../'" >> $HOME/.bashrc
    echo "export PS1=\"\\[\\033[1;93m\\]\\W \\u\\\$ \\[\\033[0m\\]\"" >> $HOME/.bashrc
    source $HOME/.bashrc
}

prepareSSH() {
    printInfo 0 "Configuring SSH..."
    printInfo 4 "Modifying /etc/ssh/sshd_config to allow root login..."
    sudo sed -i "s/^PermitRootLogin .*$/PermitRootLogin yes/g" /etc/ssh/sshd_config
    sudo service ssh restart
    printInfo 4 "Configuring SSH keys..."
    if [ ! -d $HOME/.ssh ]; then
        printInfo 8 "$HOME/.ssh was not found, creating a new one."
        mkdir -p $HOME/.ssh
        chmod 755 $HOME/.ssh
    fi
    printInfo 8 "public key"
    echo "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCjk2i49NQzXjMtXAe0LmTwlGxIIEWi9I//zw5UoCPGR4+fvGV5fF0JSQmV3MOMgheswJVh5uQuENRdlRgel/JSNvexUmvXtZU+dkIIp1G2x6ft9M4oF0Jn2ZDZdKDH3j/f33ZOUExYvfPZnpf+YoOC2nK0MkE3y/BEdexg4zGA/99RXmTdcmsnqA5Sy0Xk0Rj4yuqnNNhcqVihf15A2Zs4h4MZogdvlzaTL9y08RlDrFEQjRkXRlqathag47opU1HVZucvhViLLeaXHLUVbClALjMGvcrJxIX3cyGw0cdZ6iNdS3HmNu09trp1xZPMOKADeEOQvwGOvumr82d6dfSL dbms" \
        > $HOME/.ssh/id_rsa.pub
    chmod 640 $HOME/.ssh/id_rsa.pub
    printInfo 8 "private key"
    echo "-----BEGIN RSA PRIVATE KEY-----" > $HOME/.ssh/id_rsa
    echo "MIIEpAIBAAKCAQEAo5NouPTUM14zLVwHtC5k8JRsSCBFovSP/88OVKAjxkePn7xleXxdCUkJldzD" >> $HOME/.ssh/id_rsa
    echo "jIIXrMCVYebkLhDUXZUYHpfyUjb3sVJr17WVPnZCCKdRtsen7fTOKBdCZ9mQ2XSgx94/3992TlBM" >> $HOME/.ssh/id_rsa
    echo "WL3z2Z6X/mKDgtpytDJBN8vwRHXsYOMxgP/fUV5k3XJrJ6gOUstF5NEY+MrqpzTYXKlYoX9eQNmb" >> $HOME/.ssh/id_rsa
    echo "OIeDGaIHb5c2ky/ctPEZQ6xREI0ZF0ZamrYWoOO6KVNR1WbnL4VYiy3mlxy1FWwpQC4zBr3KycSF" >> $HOME/.ssh/id_rsa
    echo "93MhsNHHWeojXUtx5jbtPba6dcWTzDigA3hDkL8Bjr7pq/NnenX0iwIDAQABAoIBAQCKdjX4Ee4p" >> $HOME/.ssh/id_rsa
    echo "yTxC0hsHTxpMdyatavvnM2NNI0S7J48AwQR3Ly8qRbYvLO77NxMkNd66cY5BgAN4ZvCMgq8W/oG1" >> $HOME/.ssh/id_rsa
    echo "TCggpmb5AhDh/ZJp5tAJizm/+DAlyYKBi63MFKggZyXkaDCpm9j0aV4nwNJiF5vAwQ7FL6D7DRtF" >> $HOME/.ssh/id_rsa
    echo "qBg97zKxQQgWGe0Uz73yoDe5iH1mXjMj8jD2PtxiHBpry7R+5e9NP5bl950P36Cms1BiF5EHgkv7" >> $HOME/.ssh/id_rsa
    echo "0BkbRNxrfMhZy8DWtL5PKSLJeiAdJF3p///hYcl0AkmPxqZw/hUCSmlDyeNqaDi92Wc7jMHE/Xp+" >> $HOME/.ssh/id_rsa
    echo "eP5usD1fCPI+s4Q3jvVqU64Dh0BxAoGBAOwg/SI4MRs2ouTDy7p47N5j1AY/qc88wUpu1FWcuPxF" >> $HOME/.ssh/id_rsa
    echo "BaYytCL7CTL1ZHzohHRjUIyrIpKqANGYqnrPxXaALAVMVEQBGNJweW4HJRUlCsoNKDv8MT3ZUNBM" >> $HOME/.ssh/id_rsa
    echo "1N9tfJn4DQda+DD6aX2l0L8icVBOWxfhN7P7ZR7SfRPr7loQAVmpAoGBALFXYhvM3lLaBj7DpXA/" >> $HOME/.ssh/id_rsa
    echo "BlQ9KL7Be/hrahckQ9ttnw7TBmQ8C32rhsC3N4hwMNd55HFdDvvdSxx1qS/w8M6BZl4th3Q2/Tu0" >> $HOME/.ssh/id_rsa
    echo "fMHsz4c8GreTgiqyoQLWSMZIQjwWb4kTKD7AtU+QUPeMRdcqPgfOhJCa6Ay/0juLdeqe+mVAPAUT" >> $HOME/.ssh/id_rsa
    echo "AoGBAKur5D2futv9IfrN+u0sg5G/5GNSn8OCeUkDQK6pjbgi+lN1o4+XEX4R+KfzaHExz1smBLG6" >> $HOME/.ssh/id_rsa
    echo "lXPM2Y0GR5q63sZPUyhJ3+EHUtFSsnwn/Sse27SYyrgbqex3H4D9GczbEaXaVn1NYaqvBhAgG1/2" >> $HOME/.ssh/id_rsa
    echo "R7YZ0KSyzjbVc4grTJP/9OtZAoGAXW/vDzq4AOtC0xr+Nc/fI889gpQrH05pDVxcLGbroUHqYjGI" >> $HOME/.ssh/id_rsa
    echo "1GPlkfB23/pMoZtlk502YdnM02fjBpFqL0PajdBrR/4ZGrYT7ZE6HsS6RvL/aEPJtdb1bRNxYntn" >> $HOME/.ssh/id_rsa
    echo "tM9VKwsZ/JvLLULJIX7uQ+q5yf70OJNwz1LBhCbTQzm+5eUCgYBGRKgq0NTeelfV9W/bsWKJpjgf" >> $HOME/.ssh/id_rsa
    echo "cpQHcDrTuJEZFavMBZW2FKcpgWb37X+BSSTmF02ZgiIGJJlL1QaNMl6l0rG2g6zO0GSqg4TRdOVW" >> $HOME/.ssh/id_rsa
    echo "x6xKAfC6B3Y/9OBsp30snnxhyRJSe9Ep6oLlESo3TnySkEL1sNRyHAVPunnnigRvaiRDFw==" >> $HOME/.ssh/id_rsa
    echo "-----END RSA PRIVATE KEY-----" >> $HOME/.ssh/id_rsa
    chmod 600 $HOME/.ssh/id_rsa
    printInfo 8 "authorized_keys"
    cp $HOME/.ssh/id_rsa.pub $HOME/.ssh/authorized_keys
    chmod 600 $HOME/.ssh/authorized_keys
    printInfo 8 "config"
    echo "Host *" > $HOME/.ssh/config
    echo "  StrictHostKeyChecking no" >> $HOME/.ssh/config
    echo "  UserKnownHostsFile=/dev/null" >> $HOME/.ssh/config
    chmod 600 $HOME/.ssh/config
    sudo rm -rf /root/.ssh 2>/dev/null
    sudo cp -r $HOME/.ssh /root/.ssh
}

installSciDB() {
    pushd $SCIDB_SOURCE_PATH > /dev/null
    printInfo 0 "Preparing tool chain..."
    deployment/deploy.sh prepare_toolchain localhost
    deployment/deploy.sh prepare_coordinator localhost
    sudo usermod -G `whoami` -a postgres
    chmod g+rx $HOME
    printInfo 0 "Installing SciDB..."
    deployment/deploy.sh scidb_install $localPackagePath localhost
    popd > /dev/null
    sudo chown -R `whoami`:`groups $(whoami) | cut -d' ' -f3` $SCIDB_INSTALL_PATH
}

installSciDBR() {
    printInfo 0 "Configuring SciDB-R..."
    printInfo 4 "Installing shim..."
    wget $shimPackageURL
    echo "y" | sudo gdebi $shimPackageFileName
    sudo service shimsvc start
    rm $shimPackageFileName
    printInfo 4 "Installing SciDB-R..."
    sudo chmod -R o+w /usr/local/lib/R/site-library
    echo "install.packages('devtools',repos='http://cran.us.r-project.org')" > install.R
    echo "devtools::install_github(\"Paradigm4/SciDBR\")" >> install.R
    R -f install.R
    rm install.R
}

configPostgre() {
    printInfo 0 "Configuring PostgreSQL..."
    sudo sed -i "s/host    all             all             127.0.0.1\/32            md5/host    all             all             0.0.0.0\/0            md5/g" /etc/postgresql/9.3/main/pg_hba.conf
    sudo sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/g" /etc/postgresql/9.3/main/postgresql.conf
    sudo sed -i "s/max_connections = 100/max_connections = 200/g" /etc/postgresql/9.3/main/postgresql.conf
    sudo service postgresql restart
}

echo "SciDB $SCIDB_VER automated installer"
echo -e "Created by Yiqun Zhang <contact@yzhang.io>\n"

downloadSciDB
unpackSciDBSource
installPrerequisite
configEnvVars
prepareSSH
installSciDB
installSciDBR
configPostgre
printInfo 0 "SciDB is installed successfully."
