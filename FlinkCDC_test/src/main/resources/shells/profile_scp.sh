# !/bin/sh
if [ "$SHELL" = "/bin/bash" ];
  then
    echo "your login shell is the bash \n"
    echo "SHELL is : $SHELL"
  else 
    echo "your login shell is not bash but $SHELL"
fi
# [ -f "filename" ] :判断是否是一个文件 
# [ -x "/bin/ls" ] :判断/bin/ls是否存在并有可执行权限
# { -n "$var" } : 判断$var变量是否有值
# [ "$a" = "$b" ] : 判断$a和$b是否相等
# [ -f "/etc/shadow" ] 
# echo "This PC uses shadow passwords!"
if [ -f "/etc/shadow" ];
  then
    echo "电脑密码使用隐藏字符"
  else
    echo "电脑密码没使用隐藏字符"
fi

# 本sh脚本的文件将172.32.0.67上的/etc/profile文件传输到172.32.0.[68-71] 并且source之
user="root"
password="Ict@123;"

cd /etc
i=67
while [ $i -le 70 ]
do
    ((i=$i+1))
    scp /etc/profile 172.32.0.$i:/etc/
# 使用expect脚本执行
{ 
/usr/bin/expect <<EOF
    set timeout 10
spawn ssh -t root@172.32.0.$i </etc/source_profile.sh
spawn ssh -t root@172.32.0.$i echo "source success !" >>/etc/source_tmp.log
expect {
    "yes/no" { send "yes\n";exp_continue }
    "password" { send "$password\n" }
}
EOF
}&
done
