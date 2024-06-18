for((i=0;i<=10;i++))
do
    echo"Test$i"
    go test -v -run 3A
    # 检查 go 命令的退出状态
    if [ "$?" -eq 0 ]; then
        # 测试成功
        success_count=$((success_count+1))
        echo "Test iteration $i passed."
        # 如果想保存通过的测试日志，取消下面行的注释
        rm -rf terminal.txt
        touch terminal.txt
    else
        # 测试失败
        fail_count=$((fail_count+1))
        # echo "Test iteration $i failed, check 'failure"$target"_$i.log' for details."
        rm -rf terminal.txt
        touch terminal.txt
        return
    fi
done