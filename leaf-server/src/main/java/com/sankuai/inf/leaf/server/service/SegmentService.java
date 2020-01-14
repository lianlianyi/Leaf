package com.sankuai.inf.leaf.server.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.dao.impl.IDAllocDaoImpl;
//import com.sankuai.inf.leaf.server.Constants;
import com.sankuai.inf.leaf.server.exception.InitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.Properties;

@Service("SegmentService")
public class SegmentService {
	private Logger logger = LoggerFactory.getLogger(SegmentService.class);

	private IDGen idGen;
	private DruidDataSource dataSource;

	@Value("${leaf.segment.enable}")
	private boolean LEAF_SEGMENT_ENABLE;
	@Value("${leaf.jdbc.url}")
	private String LEAF_JDBC_URL;
	@Value("${leaf.jdbc.username}")
	private String LEAF_JDBC_USERNAME;
	@Value("${leaf.jdbc.password}")
	private String LEAF_JDBC_PASSWORD;

	public SegmentService() throws SQLException, InitException {
		Properties properties = PropertyFactory.getProperties();
		boolean flag = LEAF_SEGMENT_ENABLE;
		System.out.println("打印 LEAF_SEGMENT_ENABLE:" + LEAF_SEGMENT_ENABLE + ",LEAF_JDBC_URL:" + LEAF_JDBC_URL
				+ ",LEAF_JDBC_USERNAME:" + LEAF_JDBC_USERNAME + ",LEAF_JDBC_PASSWORD:" + LEAF_JDBC_PASSWORD);
		if (flag) {
			// Config dataSource
			dataSource = new DruidDataSource();
			dataSource.setUrl(properties.getProperty(LEAF_JDBC_URL));
			dataSource.setUsername(properties.getProperty(LEAF_JDBC_USERNAME));
			dataSource.setPassword(properties.getProperty(LEAF_JDBC_PASSWORD));
			dataSource.init();
			

			// Config Dao
			IDAllocDao dao = new IDAllocDaoImpl(dataSource);

			// Config ID Gen
			idGen = new SegmentIDGenImpl();
			((SegmentIDGenImpl) idGen).setDao(dao);
			if (idGen.init()) {
				logger.info("Segment Service Init Successfully");
			} else {
				throw new InitException("Segment Service Init Fail");
			}
		} else {
			idGen = new ZeroIDGen();
			logger.info("Zero ID Gen Service Init Successfully");
		}
	}

	public Result getId(String key) {
		return idGen.get(key);
	}

	public SegmentIDGenImpl getIdGen() {
		if (idGen instanceof SegmentIDGenImpl) {
			return (SegmentIDGenImpl) idGen;
		}
		return null;
	}
}
