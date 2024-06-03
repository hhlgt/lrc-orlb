#include "proxy.h"

int main(int argc, char **argv)
{
    pid_t pid = fork();
    if (pid > 0)
    {
        exit(0);
    }
    setsid();

    std::string ip(argv[1]);
    int port = std::stoi(argv[2]);
    std::string networkcore(argv[3]);
    if (true)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }

    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../clusterinfo.xml";
    ECProject::Proxy proxy(ip, port, networkcore, config_path);
    proxy.run();
    return 0;
}