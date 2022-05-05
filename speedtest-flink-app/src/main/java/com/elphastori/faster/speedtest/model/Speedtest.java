package com.elphastori.faster.speedtest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@JsonIgnoreProperties(ignoreUnknown = true)
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class Speedtest {
    @JsonProperty(value = "time", required = true)
    private String time;

    @JsonProperty(value = "ping", required = true)
    private Double ping;

    @JsonProperty(value = "jitter", required = true)
    private Double jitter;

    @JsonProperty(value = "download", required = true)
    private Double download;

    @JsonProperty(value = "upload", required = true)
    private Double upload;

    @JsonProperty(value = "host", required = true)
    private String host;
}
