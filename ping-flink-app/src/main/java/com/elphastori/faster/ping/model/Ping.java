package com.elphastori.faster.ping.model;

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
public class Ping {
    @JsonProperty(value = "time", required = true)
    private String time;

    @JsonProperty(value = "google", required = true)
    private Double google;

    @JsonProperty(value = "facebook", required = true)
    private Double facebook;

    @JsonProperty(value = "amazon", required = true)
    private Double amazon;

    @JsonProperty(value = "host", required = true)
    private String host;
}
