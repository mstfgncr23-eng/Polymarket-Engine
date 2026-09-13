export interface AstraCalibrationState { samples: number; brierSum: number; reliability: number; }
export interface AstraSettlementObservation { predictedProbability: number; outcome: 0 | 1; }

export function updateAstraCalibration(state: AstraCalibrationState, observation: AstraSettlementObservation): AstraCalibrationState {
  const probability = Math.min(1, Math.max(0, observation.predictedProbability));
  const brier = (probability - observation.outcome) ** 2;
  const samples = state.samples + 1;
  const brierSum = state.brierSum + brier;
  return { samples, brierSum, reliability: Math.max(0, Math.min(1, 1 - brierSum / samples)) };
}

export function astraCounterfactual(input: { fairProbability: number; executablePrice: number; feePerShare: number; quantity: number }): {
  wouldHaveNetEv: number;
  positive: boolean;
} {
  const wouldHaveNetEv = (input.fairProbability - input.executablePrice - input.feePerShare) * input.quantity;
  return { wouldHaveNetEv, positive: wouldHaveNetEv > 0 };
}
