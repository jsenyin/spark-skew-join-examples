package org.apache.spark.sparkLearning.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author jsen.yin [jsen.yin@gmail.com]
 * 2019-01-26
 * @Description: <p></p>
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Course implements Serializable {

    private String cid;
    private String cname;

}
