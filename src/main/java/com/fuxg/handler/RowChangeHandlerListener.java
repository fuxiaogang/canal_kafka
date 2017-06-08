package com.fuxg.handler;

import com.alibaba.otter.canal.protocol.CanalEntry;

/**
 * Created by fuxg on 2016/10/26.
 */
public interface RowChangeHandlerListener {

    public boolean  handler(CanalEntry.Entry entry, CanalEntry.RowChange rowChange);

}
