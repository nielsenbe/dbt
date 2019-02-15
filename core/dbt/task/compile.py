import os

from dbt.compilation import compile_manifest
from dbt.loader import load_all_projects
from dbt.node_runners import CompileRunner
from dbt.node_types import NodeType
from dbt.parser.models import ModelParser
from dbt.parser.util import ParserUtils
import dbt.ui.printer

from dbt.task.runnable import GraphRunnableTask, RemoteCallable


class CompileTask(GraphRunnableTask):

    def raise_on_first_error(self):
        return True

    def build_query(self):
        return {
            "include": self.args.models,
            "exclude": self.args.exclude,
            "resource_types": NodeType.executable(),
            "tags": [],
        }

    def get_runner_type(self):
        return CompileRunner

    def task_end_messages(self, results):
        dbt.ui.printer.print_timestamped_line('Done.')


class RemoteCompileTask(CompileTask, RemoteCallable):
    METHOD_NAME = 'compileNode'

    def __init__(self, args, config):
        super(CompileTask, self).__init__(args, config)
        self.parser = None

    def _runtime_initialize(self):
        super(CompileTask, self)._runtime_initialize()
        self.parser = ModelParser(
            self.config,
            all_projects=load_all_projects(self.config),
            macro_manifest=self.manifest
        )

    def handle_request(self, name, sql):
        # print('kwargs: {}'.format(kwargs))
        if self.manifest is None:
            self._runtime_initialize()
        request_path = os.path.join(self.config.target_path, 'rpc', name)
        node_dict = {
            'name': name,
            'root_path': request_path,
            'resource_type': NodeType.Model,
            'path': name+'.sql',
            'original_file_path': request_path + '.sql',
            'package_name': self.config.project_name,
            'raw_sql': sql,
        }
        #add_new_refs
        unique_id, node = self.parser.parse_sql_node(node_dict)
        # build a new graph!
        manifest = ParserUtils.add_new_refs(self.manifest, self.config,
                                                 node)
        linker = compile_manifest(self.config, manifest)

        result = self.get_runner(node).safe_run(manifest)
        return result.serialize()
