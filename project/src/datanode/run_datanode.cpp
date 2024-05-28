#include "datanode.h"

int main(int argc, char **argv)
{
    pid_t pid = fork();
    if (pid > 0)
    {
        exit(0);
    }
    setsid();
    if (false)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

    std::string ip(argv[1]);
    int port = std::stoi(argv[2]);
    ECProject::Datanode datanode(ip, port);
    datanode.run();
    return 0;
}