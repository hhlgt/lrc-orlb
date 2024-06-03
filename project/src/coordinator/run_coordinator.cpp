#include "coordinator.h"

int main(int argc, char **argv)
{
    if (LOG_TO_FILE)
    {
        umask(0);
        close(STDIN_FILENO);
        close(STDOUT_FILENO);
        close(STDERR_FILENO);
    }
    double beta = 0.5;
    if (argc == 2)
    {
        beta = (double)std::stof(argv[1]);
    }
    char buff[256];
    getcwd(buff, 256);
    std::string cwf = std::string(argv[0]);
    std::string config_path = std::string(buff) + cwf.substr(1, cwf.rfind('/') - 1) + "/../clusterinfo.xml";
    ECProject::Coordinator coordinator("0.0.0.0", COORDINATOR_PORT, config_path, beta);
    coordinator.run();
    return 0;
}