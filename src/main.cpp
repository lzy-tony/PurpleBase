#include <unistd.h>

int main() {
    while (1) {
        chdir("runs");

        /*
            主循环里，如果有 create/drop/use db 则在这里直接处理
            如果 use db 更新 sm_manager，并且切工作路径
        */
    }

    return 0;
}