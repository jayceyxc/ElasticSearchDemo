package com.linus.es.demo.utils;

import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author yuxuecheng
 * @Title: BoolExpressionParser
 * @ProjectName demo
 * @Description: 解析bool逻辑表达式
 * @date 2019/11/22 09:11
 */
public class BoolExpressionParser {

    private static final char LEFT_PARENTHESIS_CHAR = '(';
    private static final char RIGHT_PARENTHESIS_CHAR = ')';
    private static final String LEFT_PARENTHESIS = "(";
    private static final String RIGHT_PARENTHESIS = ")";
    // 词汇之间的分隔符
//    public static final String WORD_SEPARATOR = "$";

    /**
     * 判断栈顶字符串是否为操作符，如果是则返回false，否则返回true
     * @param list 转换后的字符串列表
     * @return
     */
    private static boolean topNotOperator(List<String> list) {
        if (list.isEmpty()) {
            return true;
        }

        String lastString = list.get(list.size() - 1);
        return !lastString.matches("[\\&\\|\\!]");
    }

    public static Queue<String> convert(String express) {
        List<String> transferResult = transfer(express);
        Queue<String> stringQueue = new LinkedBlockingQueue<>();
        StringBuilder stringBuilder = new StringBuilder();
        for (String str : transferResult) {
            if (str.matches("[\\&\\!\\|\\(\\)]")) {
                if (stringBuilder.length() > 0) {
                    stringQueue.add(stringBuilder.toString());
                    stringBuilder.delete(0, stringBuilder.length());
                }
                stringQueue.add(str);
            } else {
                stringBuilder.append(str);
            }
        }

        return stringQueue;
    }

    /**
     * 将中缀表达式转换为后缀表达式（逆波兰表达式）
     *
     * @param express
     * @return
     */
    public static List<String> transfer(String express) {
        if (StringUtils.isEmpty(express)) {
            return Collections.emptyList();
        }
        Stack<String> stack = new Stack<>();
        List<String> list = new ArrayList<>();
        for (int i = 0; i < express.length(); i++) {
            if ((express.charAt(i) + "").matches("[\\&\\|\\!]")) {
                String currentOp = express.charAt(i) + "";
                //如果stack为空
                if (stack.isEmpty()) {
                    // 如果list的最后一个字符串不是操作符，则添加一个$符号用于分割中文字符串
//                    if (!(list.get(list.size() -1 ).matches("[\\&\\|\\!]"))) {
//                        list.add(WORD_SEPARATOR);
//                    }
                    stack.push(currentOp);
                    if (topNotOperator(list)) {
                        if (express.charAt(i) == '|') {
                            list.add("|");
                        } else {
                            list.add("&");
                        }
                    }
                    continue;
                }
                //不为空

                //上一个元素不为（，且当前运算符优先级小于上一个元素则，将比这个运算符优先级大或相等的操作符全部加入到队列中
                while (!stack.isEmpty() && !LEFT_PARENTHESIS.equals(stack.lastElement()) && !comparePriority(currentOp, stack.lastElement())) {
//                    list.add(WORD_SEPARATOR);
                    list.add(stack.pop());
                }

//                if (!stack.isEmpty() && LEFT_PARENTHESIS.equalsIgnoreCase(stack.lastElement())) {
//                    list.add(WORD_SEPARATOR);
//                }
                stack.push(currentOp);
                if (topNotOperator(list)) {
                    if (express.charAt(i) == '|') {
                        list.add("|");
                    } else {
                        list.add("&");
                    }
                }
            } else if (express.charAt(i) == LEFT_PARENTHESIS_CHAR) {
                //遇到左小括号无条件加入
                stack.push(express.charAt(i) + "");
                // 将左括号加入到队列中，用以区分后面的是否为括号中的内容
                list.add(LEFT_PARENTHESIS);
            } else if (express.charAt(i) == RIGHT_PARENTHESIS_CHAR) {
                //遇到右小括号，则寻找上一堆小括号，然后把中间的值全部放入队列中
                while (!(LEFT_PARENTHESIS).equals(stack.lastElement())) {
//                    list.add(WORD_SEPARATOR);
                    list.add(stack.pop());
                }
                //上述循环停止，这栈顶元素必为"("
                stack.pop();
                // 将右括号加入到队列中，用以区分前面的是否为括号中的内容
                list.add(RIGHT_PARENTHESIS);
            } else {
                list.add(express.charAt(i) + "");
            }
        }
        //将栈中剩余元素加入到队列中
        while (!stack.isEmpty()) {
//            list.add(WORD_SEPARATOR);
            list.add(stack.pop());
        }

        return list;
    }

    /**
     * 比较运算符的优先级
     *
     * @param o1 运算符1
     * @param o2 运算符2
     * @return 运算符的优先级比较结果，如果o1的优先级高，返回true，否则返回false
     */
    private static boolean comparePriority(String o1, String o2) {
        return getPriorityValue(o1) > getPriorityValue(o2);
    }

    /**
     * 获得运算符的优先级，数字越大，优先级越高。
     *
     * @param str 运算符
     * @return 返回运算符的优先级
     */
    private static int getPriorityValue(String str) {
        switch (str) {
            case "!":
                return 3;
            case "&":
                return 2;
            case "|":
                return 1;
            default:
                throw new UnsupportedOperationException("没有该类型的运算符！");
        }
    }

    public static void main(String[] args) {
        StringBuilder stringBuilder = new StringBuilder();
        String str = "(华法林&达比加群&利伐沙班)|肌钙蛋白";
        List<String> result = transfer(str);
        for (String s : result) {
            stringBuilder.append(s);
        }
        System.out.println("原始字符串：" + str + "，转换后：" + stringBuilder.toString());
        Queue<String> convertQueue = convert(str);
        System.out.println("原始字符串：" + str + "，转换后：" + convertQueue);
        str = "华法林&达比加群";
        result = transfer(str);
        System.out.println("原始字符串：" + str + "，转换后：" + result);
        str = "华法林!达比加群";
        result = transfer(str);
        System.out.println("原始字符串：" + str + "，转换后：" + result);
        str = "(华法林|达比加群)&利伐沙班&肌钙蛋白";
        result = transfer(str);
        System.out.println("原始字符串：" + str + "，转换后：" + result);
        str = "(华法林|达比加群|()）)&利伐沙班&肌钙蛋白";
        result = transfer(str);
        System.out.println("原始字符串：" + str + "，转换后：" + result);
    }
}
