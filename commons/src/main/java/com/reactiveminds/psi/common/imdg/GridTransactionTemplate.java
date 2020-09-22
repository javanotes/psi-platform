package com.reactiveminds.psi.common.imdg;

import com.reactiveminds.psi.client.GridTransaction;
import com.reactiveminds.psi.client.GridTransactionContext;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;

class GridTransactionTemplate implements GridTransaction {
    @Autowired
    BeanFactory beanFactory;

    @Override
    public GridTransactionContext newTransaction() {
        return beanFactory.getBean(GridTransactionContext.class);
    }
}
