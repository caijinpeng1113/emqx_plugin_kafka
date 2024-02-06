# emqx_plugin_kafka

Kafka plugin for EMQX >= V5.4.0，支持EMQX转Kafka配置SASL/PLAIN模式。

## Usage

### Build the EMQX broker
* 先安装相关依赖组件，本文基于RockyLinux 8.5 环境，通过dnf方式安装
```shell
dnf -y install gcc gcc-c++ cpp glibc  glibc-devel glibc-headers kernel-devel kernel-headers cmake make m4 ncurses ncurses-devel openssl openssl-devel openssl-libs zlib zlib-devel libselinux-devel xmlto perl git wget zip unzip gtk2-devel binutils-devel unixODBC unixODBC-devel libtool  libtool-ltdl-devel  wxWidgets bzip2 binutils-devel
```

* 安装Erlang/OTP   ( emqx v5.4.0 安装 25.2.2 )

```
解压 Erlang
tar xvf  otp_src_25.2.2.tar.gz 
 
进入解压目录
cd /opt/otp_src_25.2.2

编译  
./configure --prefix=/usr/local/erlang --with-ssl --enable-threads --enable-smp-support --enable-kernel-poll --enable-hipe --without-javac

编译并安装
make && make install

修改环境变量
vim /etc/profile
 
将以下内容存储至profile文件中，保存并退出
export ERLPATH=/usr/local/erlang
export PATH=$ERLPATH/bin:$PATH
 
使环境变量刷新并生效
source /etc/profile
 
验证erlang是否安装成功
erl

```

* 安装 Rebar3    (  rebar3 安装 3.20.0 )

```
解压：  
tar -xvf  rebar3-3.20.0.tar.gz 

进入解压目录
cd rebar3-3.20.0

修改 rebar3/rebar.config,  文件末尾添加 {plugins, [rebar3_hex]}.

编译
./bootstrap
./rebar3 local install

添加到PATH
vi ~/.bashrc
export PATH=$PATH:~/.cache/rebar3/bin
source ~/.bashrc   

验证版本 
rebar3 -v
rebar3 hex
```

* 编译EMQX Broker

```
拷贝源码：  git clone -b v5.4.0  https://github.com/emqx/emqx.git emqx-v5.4.0
编译执行：  export BUILD_WITHOUT_QUIC=1;make
```



### Release

```shell
> git clone https://github.com/caijinpeng1113/emqx_plugin_kafka.git
> cd emqx_plugin_kafka
> make rel
_build/default/emqx_plugrel/emqx_plugin_kafka-<vsn>.tar.gz
```

### Config

#### 将编译后emqx_plugin_kafka插件包，通过EMQX的插件进行安装。
#### 在EMQX的目录下，增加配置文件，如下： `emqx/etc/emqx_plugin_kafka.hocon` ，然后进行启动。

```shell
plugin_kafka {
  connection {
    bootstrap_hosts = ["10.3.64.223:9192", "10.3.64.223:9292", "10.3.64.223:9392"]
    sasl {
      // enum:  plain | scram_sha_256 | scram_sha_512
      mechanism = plain
      username = "admin"
      password = "admin"
    }
    ssl {
      enable = false
    }
  }
  
  // create kafka topic mqtt_data and emqx_test
  hooks = [
    {endpoint = client.connect}
    , {endpoint = client.connack}
    , {endpoint = client.connected, kafka_topic = mqtt_data}
    , {endpoint = client.disconnected, kafka_topic = mqtt_data}
    , {endpoint = client.authenticate}
    , {endpoint = client.authorize}
    , {endpoint = client.authenticate}
    , {endpoint = client.check_authz_complete}
    , {endpoint = session.created}
    , {endpoint = session.subscribed}
    , {endpoint = session.unsubscribed}
    , {endpoint = session.resumed}
    , {endpoint = session.discarded}
    , {endpoint = session.takenover}
    , {endpoint = session.terminated}
    , {endpoint = message.publish, kafka_topic = mqtt_data,  filter = "sys/#"}
    , {endpoint = message.delivered, kafka_topic = mqtt_data, filter = "sys/#"}
    , {endpoint = message.acked, filter = "sys/#"}
    , {endpoint = message.dropped, filter = "sys/#"}
  ]
}

```

Some examples in the directory `priv/example/`.

#### Hook Point

|          endpoint           |  filter  |
| :-------------------------: | :------: |
|       client.connect        |    /     |
|       client.connack        |    /     |
|      client.connected       |    /     |
|     client.disconnected     |    /     |
|     client.authenticate     |    /     |
|      client.authorize       |    /     |
|     client.authenticate     |    /     |
| client.check_authz_complete |    /     |
|       session.created       |    /     |
|     session.subscribed      |    /     |
|    session.unsubscribed     |    /     |
|       session.resumed       |    /     |
|      session.discarded      |    /     |
|      session.takenover      |    /     |
|     session.terminated      |    /     |
|       message.publish       | required |
|      message.delivered      | required |
|        message.acked        | required |
|       message.dropped       | required |

#### Path

- Default path： `emqx/etc/emqx_plugin_kafka.hocon`
- Attach to path:  set system environment variables  `export EMQX_PLUGIN_KAFKA_CONF="absolute_path"`


![1111111](https://github.com/caijinpeng1113/emqx_plugin_kafka/assets/158483689/2adb3532-13cc-4090-895a-1d1c8c28ae84)




