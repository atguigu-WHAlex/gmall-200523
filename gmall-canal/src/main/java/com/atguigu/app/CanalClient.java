package com.atguigu.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.constants.GmallConstant;
import com.atguigu.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {

        //1.创建Canal连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(
                        "hadoop102",
                        11111),
                "example",
                "",
                "");

        while (true) {

            canalConnector.connect();
            canalConnector.subscribe("gmall200523.*");

            //抓取数据
            Message message = canalConnector.get(100);

            if (message.getEntries().size() <= 0) {
                System.out.println("当前没有数据,休息一下！！！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {

                //解析message
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断类型,如果为非ROWDATA类型,则不进行解析
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        try {
                            //获取表名
                            String tableName = entry.getHeader().getTableName();
                            //获取数据
                            ByteString storeValue = entry.getStoreValue();
                            //反序列化storeValue
                            CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                            //获取操作数据类型
                            CanalEntry.EventType eventType = rowChange.getEventType();
                            //获取数据集
                            List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();

                            //处理数据,根据类型及表名,将数据发送至Kafka
                            handler(tableName, eventType, rowDatasList);

                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }

                    }

                }

            }

        }

    }

    //处理数据,根据类型及表名,将数据发送至Kafka
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {

        //订单表,只需要新增数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                //打印数据,并将数据发送Kafka
                System.out.println(jsonObject);
                MyKafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER_INFO, jsonObject.toString());

            }

        }


    }

}
