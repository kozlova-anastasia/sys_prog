#include "parser.h"

#include <assert.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include <vector>
#include <string>

static int last_exit_code = 0;

static int run_pipeline(
    const std::vector<const command*>& cmds,
    const command_line *line,
    bool apply_redirect)
{
    int n = cmds.size();

    std::vector<int> pipes(2 * (n - 1));
    std::vector<pid_t> pids;

    for (int i = 0; i < n - 1; i++)
        pipe(&pipes[2 * i]);

    for (int i = 0; i < n; i++) {

        pid_t pid = fork();

        if (pid == 0) {

            if (i > 0)
                dup2(pipes[2 * (i - 1)], STDIN_FILENO);

            if (i < n - 1)
                dup2(pipes[2 * i + 1], STDOUT_FILENO);
            else if (apply_redirect) {

                if (line->out_type == OUTPUT_TYPE_FILE_NEW) {
                    int fd = open(line->out_file.c_str(),
                                  O_CREAT | O_WRONLY | O_TRUNC, 0644);
                    dup2(fd, STDOUT_FILENO);
                    close(fd);
                }

                else if (line->out_type == OUTPUT_TYPE_FILE_APPEND) {
                    int fd = open(line->out_file.c_str(),
                                  O_CREAT | O_WRONLY | O_APPEND, 0644);
                    dup2(fd, STDOUT_FILENO);
                    close(fd);
                }
            }

            for (int fd : pipes)
                close(fd);

            const command *cmd = cmds[i];

            if (cmd->exe == "exit") {
                int code = 0;
                if (!cmd->args.empty())
                    code = atoi(cmd->args[0].c_str());
                _exit(code);
            }

            std::vector<char*> argv;
            argv.push_back((char*)cmd->exe.c_str());

            for (const std::string &a : cmd->args)
                argv.push_back((char*)a.c_str());

            argv.push_back(NULL);

            execvp(cmd->exe.c_str(), argv.data());

            _exit(1);
        }

        pids.push_back(pid);
    }

    for (int fd : pipes)
        close(fd);

    int status = 0;

    for (pid_t pid : pids)
        waitpid(pid, &status, 0);

    return WEXITSTATUS(status);
}

static void execute_command_line(const command_line *line)
{
    assert(line != NULL);

    std::vector<const command*> pipeline;

    int mode = 0; // 0 none, 1 AND, 2 OR

    size_t total_cmds = 0;
    for (const expr &e : line->exprs)
        if (e.type == EXPR_TYPE_COMMAND)
            total_cmds++;

    size_t processed = 0;

    for (const expr &e : line->exprs) {

        if (e.type == EXPR_TYPE_COMMAND) {
            pipeline.push_back(&(*e.cmd));
        }

        else if (e.type == EXPR_TYPE_PIPE) {
            continue;
        }

        else if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {

            bool run = true;

            if (mode == 1 && last_exit_code != 0)
                run = false;

            if (mode == 2 && last_exit_code == 0)
                run = false;

            if (run) {

                if (pipeline.size() == 1) {

                    const command *cmd = pipeline[0];

                    if (cmd->exe == "cd") {

                        if (!cmd->args.empty())
                            chdir(cmd->args[0].c_str());
                        else
                            chdir(getenv("HOME"));

                        last_exit_code = 0;
                    }

                    else if (cmd->exe == "exit") {

                        int code = last_exit_code;

                        if (!cmd->args.empty())
                            code = atoi(cmd->args[0].c_str());

                        exit(code);
                    }

                    else {

                        processed += pipeline.size();
                        bool last = (processed == total_cmds);

                        last_exit_code =
                            run_pipeline(pipeline, line, last);
                    }
                }

                else {

                    processed += pipeline.size();
                    bool last = (processed == total_cmds);

                    last_exit_code =
                        run_pipeline(pipeline, line, last);
                }
            }

            pipeline.clear();

            mode = (e.type == EXPR_TYPE_AND) ? 1 : 2;
        }
    }

    if (!pipeline.empty()) {

        bool run = true;

        if (mode == 1 && last_exit_code != 0)
            run = false;

        if (mode == 2 && last_exit_code == 0)
            run = false;

        if (run) {

            if (pipeline.size() == 1) {

                const command *cmd = pipeline[0];

                if (cmd->exe == "cd") {

                    if (!cmd->args.empty())
                        chdir(cmd->args[0].c_str());
                    else
                        chdir(getenv("HOME"));

                    last_exit_code = 0;
                    return;
                }

                if (cmd->exe == "exit") {

                    int code = last_exit_code;

                    if (!cmd->args.empty())
                        code = atoi(cmd->args[0].c_str());

                    exit(code);
                }
            }

            processed += pipeline.size();
            bool last = (processed == total_cmds);

            last_exit_code =
                run_pipeline(pipeline, line, last);
        }
    }
}

int main()
{
    const size_t buf_size = 1024;
    char buf[buf_size];

    struct parser *p = parser_new();

    int rc;

    while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {

        parser_feed(p, buf, rc);

        struct command_line *line = NULL;

        while (true) {

            enum parser_error err = parser_pop_next(p, &line);

            if (err == PARSER_ERR_NONE && line == NULL)
                break;

            if (err != PARSER_ERR_NONE)
                continue;

            execute_command_line(line);

            delete line;
        }
    }

    parser_delete(p);

    return last_exit_code;
}