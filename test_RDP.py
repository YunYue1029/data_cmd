"""
測試遞歸下降語法解析器的輸出結果

這個腳本展示 CommandParser 如何將命令字串解析成 AST (抽象語法樹)
"""

import json
from RDP.parser.command_parser import CommandParser, ParserError
from RDP.syntax_tree.nodes import (
    CommandAST,
    SourceNode,
    PipeCommandNode,
    FunctionCallNode,
    SubqueryNode,
    LiteralNode,
    IdentifierNode,
    BinaryOpNode,
    KeywordArgumentNode,
    PositionalArgumentNode,
)


def print_ast_tree(ast: CommandAST, indent: int = 0) -> None:
    """以樹狀結構打印 AST"""
    prefix = "  " * indent
    
    print(f"{prefix}CommandAST:")
    print(f"{prefix}  Source:")
    _print_source(ast.source, indent + 2)
    
    if ast.pipe_chain:
        print(f"{prefix}  Pipe Chain:")
        for i, pipe_cmd in enumerate(ast.pipe_chain):
            print(f"{prefix}    [{i+1}] {pipe_cmd.name}:")
            _print_pipe_command(pipe_cmd, indent + 4)


def _print_source(source: SourceNode, indent: int) -> None:
    """打印 Source 節點"""
    prefix = "  " * indent
    print(f"{prefix}Type: {source.source_type}")
    print(f"{prefix}Name: {source.source_name}")
    
    if source.parameters:
        print(f"{prefix}Parameters:")
        for k, v in source.parameters.items():
            print(f"{prefix}  {k} = {v}")
    
    if source.multi_sources:
        print(f"{prefix}Multi Sources:")
        for src in source.multi_sources:
            _print_source(src, indent + 2)


def _print_pipe_command(cmd: PipeCommandNode, indent: int) -> None:
    """打印 PipeCommand 節點"""
    prefix = "  " * indent
    
    if cmd.arguments:
        print(f"{prefix}Arguments:")
        for arg in cmd.arguments:
            _print_argument(arg, indent + 2)
    
    if cmd.aggregations:
        print(f"{prefix}Aggregations:")
        for agg in cmd.aggregations:
            _print_function_call(agg, indent + 2)
    
    if cmd.by_fields:
        print(f"{prefix}By Fields: {', '.join(cmd.by_fields)}")
    
    if cmd.subqueries:
        print(f"{prefix}Subqueries:")
        for subq in cmd.subqueries:
            _print_subquery(subq, indent + 2)


def _print_argument(arg, indent: int) -> None:
    """打印參數節點"""
    prefix = "  " * indent
    
    if isinstance(arg, KeywordArgumentNode):
        print(f"{prefix}KeywordArg: {arg.key} = ", end="")
        _print_value(arg.value, indent + len(f"KeywordArg: {arg.key} = "))
    elif isinstance(arg, PositionalArgumentNode):
        print(f"{prefix}PositionalArg: ", end="")
        _print_value(arg.value, indent + len("PositionalArg: "))


def _print_value(value, indent: int) -> None:
    """打印值節點"""
    if isinstance(value, LiteralNode):
        print(f"Literal({value.value!r}, type={value.literal_type})")
    elif isinstance(value, IdentifierNode):
        print(f"Identifier({value.name})")
    elif isinstance(value, BinaryOpNode):
        print(f"BinaryOp({value.operator})")
        print("  " * indent + "  Left: ", end="")
        _print_value(value.left, indent + 2)
        print("  " * indent + "  Right: ", end="")
        _print_value(value.right, indent + 2)
    elif isinstance(value, FunctionCallNode):
        _print_function_call(value, indent)
    else:
        print(f"{type(value).__name__}({value})")


def _print_function_call(func: FunctionCallNode, indent: int) -> None:
    """打印函數調用節點"""
    prefix = "  " * indent
    print(f"{prefix}FunctionCall: {func.name}")
    if func.arguments:
        print(f"{prefix}  Arguments:")
        for arg in func.arguments:
            print(f"{prefix}    - ", end="")
            _print_value(arg, indent + 4)


def _print_subquery(subq: SubqueryNode, indent: int) -> None:
    """打印子查詢節點"""
    prefix = "  " * indent
    print(f"{prefix}Subquery:")
    print_ast_tree(subq.command, indent + 1)


def test_parser(command: str, description: str = "") -> None:
    """測試解析器並顯示結果"""
    print("=" * 80)
    if description:
        print(f"測試: {description}")
    print(f"命令: {command}")
    print("=" * 80)
    
    try:
        parser = CommandParser(command)
        ast = parser.parse()
        
        print("\n【AST 結構】")
        print_ast_tree(ast)
        
        print("\n【簡化表示】")
        print(repr(ast))
        
        print("\n【JSON 格式】")
        print(json.dumps(_ast_to_dict(ast), indent=2, ensure_ascii=False))
        
    except ParserError as e:
        print(f"\n❌ 解析錯誤: {e}")
    except Exception as e:
        print(f"\n❌ 錯誤: {type(e).__name__}: {e}")
    
    print("\n")


def _ast_to_dict(node) -> dict:
    """將 AST 轉換為字典以便 JSON 序列化"""
    if isinstance(node, CommandAST):
        return {
            "type": "CommandAST",
            "source": _ast_to_dict(node.source) if node.source else None,
            "pipe_chain": [_ast_to_dict(cmd) for cmd in node.pipe_chain],
        }
    elif isinstance(node, SourceNode):
        result = {
            "type": "SourceNode",
            "source_type": node.source_type,
            "source_name": node.source_name,
        }
        if node.parameters:
            result["parameters"] = node.parameters
        if node.multi_sources:
            result["multi_sources"] = [_ast_to_dict(s) for s in node.multi_sources]
        return result
    elif isinstance(node, PipeCommandNode):
        result = {
            "type": "PipeCommandNode",
            "name": node.name,
            "arguments": [_ast_to_dict(arg) for arg in node.arguments],
        }
        if node.by_fields:
            result["by_fields"] = node.by_fields
        if node.aggregations:
            result["aggregations"] = [_ast_to_dict(agg) for agg in node.aggregations]
        if node.subqueries:
            result["subqueries"] = [_ast_to_dict(subq) for subq in node.subqueries]
        return result
    elif isinstance(node, KeywordArgumentNode):
        return {
            "type": "KeywordArgumentNode",
            "key": node.key,
            "value": _ast_to_dict(node.value),
        }
    elif isinstance(node, PositionalArgumentNode):
        return {
            "type": "PositionalArgumentNode",
            "value": _ast_to_dict(node.value),
        }
    elif isinstance(node, FunctionCallNode):
        return {
            "type": "FunctionCallNode",
            "name": node.name,
            "arguments": [_ast_to_dict(arg) for arg in node.arguments],
        }
    elif isinstance(node, SubqueryNode):
        return {
            "type": "SubqueryNode",
            "command": _ast_to_dict(node.command),
        }
    elif isinstance(node, LiteralNode):
        return {
            "type": "LiteralNode",
            "value": node.value,
            "literal_type": node.literal_type,
        }
    elif isinstance(node, IdentifierNode):
        return {
            "type": "IdentifierNode",
            "name": node.name,
        }
    elif isinstance(node, BinaryOpNode):
        return {
            "type": "BinaryOpNode",
            "operator": node.operator,
            "left": _ast_to_dict(node.left),
            "right": _ast_to_dict(node.right),
        }
    else:
        return {"type": type(node).__name__, "value": str(node)}


if __name__ == "__main__":
    print("\n" + "=" * 80)
    print("遞歸下降語法解析器測試")
    print("=" * 80 + "\n")
    
    # 測試 1: 簡單的 cache 命令
    test_parser(
        'cache=test_data',
        "簡單的 cache 來源"
    )
    
    # 測試 2: 帶 pipe 的命令
    test_parser(
        'cache=test_data | head 10',
        "帶 head 命令的管道"
    )
    
    # 測試 3: stats 命令
    test_parser(
        'cache=sales | stats sum(amount) as total, count as n by department',
        "stats 聚合命令"
    )
    
    # 測試 4: 複雜的 stats 命令
    test_parser(
        'cache=orders | stats avg(amount) as avg_amount, max(amount) as max_amount, min(amount) as min_amount by customer_id',
        "多個聚合函數的 stats 命令"
    )
    
    # 測試 5: filter 命令
    test_parser(
        'cache=test_data | filter count <= 30 and status == "ok"',
        "filter 命令與布林表達式"
    )
    
    # 測試 6: eval 命令
    test_parser(
        'cache=test_data | eval total = amount * quantity, discount = amount * 0.1',
        "eval 命令與算術表達式"
    )
    
    # 測試 7: join 命令與子查詢
    test_parser(
        'cache=test_data | join customer_id [search index="customers" | stats first(segment) as segment, first(region) as region by customer_id]',
        "join 命令與子查詢"
    )
    
    # 測試 8: search 來源
    test_parser(
        'search index="logs" latest=-5m earliest="2024-01-01" | stats count by status',
        "search 來源與時間參數"
    )
    
    # 測試 9: 多來源查詢
    test_parser(
        '(index="a" OR index="b") | stats count',
        "多來源查詢 (OR)"
    )
    
    # 測試 10: sort 命令
    test_parser(
        'cache=test_data | sort -amount, customer_id',
        "sort 命令（降序與升序）"
    )
    
    # 測試 11: bucket 命令
    test_parser(
        'cache=test_data | bucket timestamp span=5m',
        "bucket 命令與時間跨度"
    )
    
    # 測試 12: transaction 命令
    test_parser(
        'cache=test_data | transaction session_id maxspan=1h',
        "transaction 命令"
    )
    
    # 測試 13: where 命令
    test_parser(
        'cache=test_data | where status_code >= 400 AND status_code < 500',
        "where 命令與複雜條件"
    )
    
    # 測試 14: 複雜的管道鏈
    test_parser(
        'cache=test_data | fields + col_*',
        "複雜的管道鏈（多個命令）"
    )
    
    print("\n" + "=" * 80)
    print("測試完成！")
    print("=" * 80 + "\n")
